// Package adapterhttp provides the mandatory public HTTP boundary shared by
// the ATOS A2A and MCP adapters. It authenticates transport access only; it
// never decides payment, identity, execution, or settlement semantics.
package adapterhttp

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
)

const (
	DefaultMaxRequestBytes = int64(16 << 20)
	DefaultMaxConcurrent   = 64
	maximumRequestBytes    = int64(16 << 20)
	maximumConcurrent      = 1024
)

type BoundaryConfig struct {
	BearerToken     string
	MaxRequestBytes int64
	MaxConcurrent   int
}

func NewBoundary(next http.Handler, config BoundaryConfig) (http.Handler, error) {
	config.BearerToken = strings.TrimSpace(config.BearerToken)
	if next == nil || len(config.BearerToken) < 32 || len(config.BearerToken) > 512 ||
		strings.ContainsAny(config.BearerToken, "\r\n\t ") {
		return nil, errors.New("a strong bounded adapter bearer token is required")
	}
	if config.MaxRequestBytes == 0 {
		config.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = DefaultMaxConcurrent
	}
	if config.MaxRequestBytes <= 0 || config.MaxRequestBytes > maximumRequestBytes ||
		config.MaxConcurrent <= 0 || config.MaxConcurrent > maximumConcurrent {
		return nil, errors.New("invalid public adapter resource bounds")
	}
	semaphore := make(chan struct{}, config.MaxConcurrent)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Referrer-Policy", "no-referrer")
		if request == nil || request.Header.Get("Origin") != "" {
			http.Error(writer, "browser-origin requests are not accepted", http.StatusForbidden)
			return
		}
		provided, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(provided), []byte(config.BearerToken)) != 1 {
			writer.Header().Set("WWW-Authenticate", `Bearer realm="atos-native-provider"`)
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		select {
		case semaphore <- struct{}{}:
			defer func() { <-semaphore }()
		default:
			writer.Header().Set("Retry-After", "1")
			http.Error(writer, "adapter concurrency limit reached", http.StatusTooManyRequests)
			return
		}
		if request.ContentLength > config.MaxRequestBytes {
			http.Error(writer, "request body is too large", http.StatusRequestEntityTooLarge)
			return
		}
		request.Body = http.MaxBytesReader(writer, request.Body, config.MaxRequestBytes)
		next.ServeHTTP(writer, request)
	}), nil
}
