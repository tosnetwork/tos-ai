package fleetcontrol

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/tosnetwork/tos-protocol/pkg/identity"
)

const (
	TransportPath       = "/tos-ai/fleet/v1/commands"
	MaxTransportBody    = 64 << 10
	MaxTransportToken   = 8 << 10
	MaxTransportWorkers = 128
)

type TransportConfig struct {
	Agent         *Agent
	BearerToken   string
	Now           func() time.Time
	MaxConcurrent int
}

// NewTransportHandler exposes the signed Agent state machine through a
// separately authenticated, bounded HTTP handler. Deployments must mount it
// behind TLS or an equivalently private authenticated channel.
func NewTransportHandler(config TransportConfig) (http.Handler, error) {
	if config.Agent == nil || config.Now == nil || config.MaxConcurrent <= 0 ||
		config.MaxConcurrent > MaxTransportWorkers || !validTransportToken(config.BearerToken) {
		return nil, errors.New("invalid fleet transport configuration")
	}
	gate := make(chan struct{}, config.MaxConcurrent)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if request.Method != http.MethodPost || request.URL.Path != TransportPath ||
			request.URL.RawQuery != "" || request.Header.Get("Content-Type") != "application/json" ||
			request.Header.Get("Content-Encoding") != "" {
			http.Error(writer, "request rejected", http.StatusBadRequest)
			return
		}
		expected := []byte("Bearer " + config.BearerToken)
		provided := []byte(request.Header.Get("Authorization"))
		if len(expected) != len(provided) || subtle.ConstantTimeCompare(expected, provided) != 1 {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		select {
		case gate <- struct{}{}:
			defer func() { <-gate }()
		default:
			writer.Header().Set("Retry-After", "1")
			http.Error(writer, "capacity exhausted", http.StatusServiceUnavailable)
			return
		}
		data, err := io.ReadAll(io.LimitReader(request.Body, MaxTransportBody+1))
		if err != nil || len(data) == 0 || len(data) > MaxTransportBody || validateTransportJSON(data) != nil {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		var envelope identity.Envelope
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF {
			http.Error(writer, "invalid request", http.StatusBadRequest)
			return
		}
		result, err := config.Agent.Submit(request.Context(), envelope, config.Now().UTC())
		if err != nil {
			http.Error(writer, "command rejected", http.StatusConflict)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(result)
	}), nil
}

func validTransportToken(value string) bool {
	if len(value) < 16 || len(value) > MaxTransportToken {
		return false
	}
	for _, character := range value {
		if character <= ' ' || character == 0x7f {
			return false
		}
	}
	return true
}

func validateTransportJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	return consumeTransportJSON(decoder, 0)
}

func consumeTransportJSON(decoder *json.Decoder, depth int) error {
	if depth > 8 {
		return errors.New("JSON depth exceeded")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	if delimiter != '{' && delimiter != '[' {
		return errors.New("invalid JSON")
	}
	keys := make(map[string]struct{}, 16)
	for decoder.More() {
		if delimiter == '{' {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON key")
			}
			if _, duplicate := keys[key]; duplicate {
				return errors.New("duplicate JSON key")
			}
			keys[key] = struct{}{}
		}
		if err := consumeTransportJSON(decoder, depth+1); err != nil {
			return err
		}
	}
	end, err := decoder.Token()
	if err != nil || (delimiter == '{' && end != json.Delim('}')) ||
		(delimiter == '[' && end != json.Delim(']')) {
		return errors.New("invalid JSON")
	}
	if depth == 0 {
		if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
			return errors.New("multiple JSON values")
		}
	}
	return nil
}
