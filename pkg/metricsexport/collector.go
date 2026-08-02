package metricsexport

import (
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	CollectorPath              = "/tos-ai/metrics/v1/ingest"
	MaxTerminalsHard           = 4096
	MaxCollectorTTL            = 24 * time.Hour
	MaxCollectorConcurrent     = 128
	DefaultCollectorConcurrent = 16
)

type CollectorConfig struct {
	// Credentials maps a privacy-minimized operator alias to a dedicated
	// bearer token. Neither value may be a hostname or hardware identifier.
	Credentials map[string]string
	TTL         time.Duration
	Now         func() time.Time
	// MaxConcurrent bounds authenticated requests before any body is read.
	// Zero selects DefaultCollectorConcurrent.
	MaxConcurrent int
}

type TerminalSnapshot struct {
	Alias       string
	CollectedAt time.Time
	Metrics     []byte
}

type Collector struct {
	mu          sync.Mutex
	credentials map[[sha256.Size]byte]string
	snapshots   map[string]TerminalSnapshot
	ttl         time.Duration
	now         func() time.Time
	gate        chan struct{}
}

func NewCollector(config CollectorConfig) (*Collector, error) {
	if len(config.Credentials) == 0 || len(config.Credentials) > MaxTerminalsHard ||
		config.TTL <= 0 || config.TTL > MaxCollectorTTL || config.Now == nil {
		return nil, errors.New("invalid metrics collector configuration")
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = DefaultCollectorConcurrent
	}
	if config.MaxConcurrent < 0 || config.MaxConcurrent > MaxCollectorConcurrent {
		return nil, errors.New("invalid metrics collector concurrency")
	}
	credentials := make(map[[sha256.Size]byte]string, len(config.Credentials))
	for alias, token := range config.Credentials {
		if !validAlias(alias) || !validToken(token) {
			return nil, errors.New("invalid metrics collector credential")
		}
		digest := sha256.Sum256([]byte(token))
		if _, duplicate := credentials[digest]; duplicate {
			return nil, errors.New("metrics collector credentials must be unique")
		}
		credentials[digest] = alias
	}
	return &Collector{
		credentials: credentials, snapshots: make(map[string]TerminalSnapshot, len(credentials)),
		ttl: config.TTL, now: config.Now, gate: make(chan struct{}, config.MaxConcurrent),
	}, nil
}

func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if c == nil || request.Method != http.MethodPost || request.URL.Path != CollectorPath || request.URL.RawQuery != "" ||
			request.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" ||
			request.Header.Get("X-TOS-Metrics-Version") != "1" || request.Header.Get("Content-Encoding") != "" {
			http.Error(writer, "request rejected", http.StatusBadRequest)
			return
		}
		alias, ok := c.authenticate(request.Header.Get("Authorization"))
		if !ok {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		select {
		case c.gate <- struct{}{}:
			defer func() { <-c.gate }()
		default:
			writer.Header().Set("Retry-After", "1")
			http.Error(writer, "capacity exhausted", http.StatusServiceUnavailable)
			return
		}
		data, err := io.ReadAll(io.LimitReader(request.Body, MaxSnapshotBytes+1))
		if err != nil || len(data) == 0 || len(data) > MaxSnapshotBytes || !safeSnapshot(data) {
			http.Error(writer, "snapshot rejected", http.StatusBadRequest)
			return
		}
		now := c.now().UTC()
		if now.IsZero() {
			http.Error(writer, "collector unavailable", http.StatusServiceUnavailable)
			return
		}
		c.mu.Lock()
		c.pruneLocked(now)
		c.snapshots[alias] = TerminalSnapshot{Alias: alias, CollectedAt: now, Metrics: append([]byte(nil), data...)}
		c.mu.Unlock()
		writer.WriteHeader(http.StatusNoContent)
	})
}

// Latest returns a sorted defensive snapshot and performs bounded TTL cleanup.
// The collector retains at most one fixed-size payload per configured alias.
func (c *Collector) Latest(now time.Time) ([]TerminalSnapshot, error) {
	if c == nil || now.IsZero() {
		return nil, errors.New("invalid metrics snapshot request")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked(now.UTC())
	result := make([]TerminalSnapshot, 0, len(c.snapshots))
	for _, snapshot := range c.snapshots {
		snapshot.Metrics = append([]byte(nil), snapshot.Metrics...)
		result = append(result, snapshot)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].Alias < result[right].Alias })
	return result, nil
}

func (c *Collector) authenticate(header string) (string, bool) {
	provided := strings.TrimPrefix(header, "Bearer ")
	if len(provided) == len(header) || !validToken(provided) {
		return "", false
	}
	// Retain only a fixed-size one-way digest of each bearer token. Tokens are
	// required to be high-entropy operator secrets, not passwords.
	digest := sha256.Sum256([]byte(provided))
	alias, ok := c.credentials[digest]
	return alias, ok
}

func (c *Collector) pruneLocked(now time.Time) {
	for alias, snapshot := range c.snapshots {
		if !snapshot.CollectedAt.Add(c.ttl).After(now) {
			delete(c.snapshots, alias)
		}
	}
}

func validAlias(value string) bool {
	if len(value) < 3 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') && character != '-' {
			return false
		}
	}
	return true
}

func validToken(value string) bool {
	return len(value) >= 16 && len(value) <= MaxTokenBytes && strings.IndexFunc(value, func(character rune) bool { return character <= ' ' || character == 0x7f }) < 0
}
