package fleetcontrol

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFleetTransportAuthenticatesBoundsAndExecutesSignedCommand(t *testing.T) {
	publicKey, privateKey := fleetKey(t)
	online, busy := true, false
	executor := &mockExecutor{}
	agent := openTestAgent(t, "terminal-one", publicKey, executor, &online, &busy)
	now := time.Unix(1_800_000_000, 0)
	handler, err := NewTransportHandler(TransportConfig{
		Agent: agent, BearerToken: "separate-operator-token", Now: func() time.Time { return now }, MaxConcurrent: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope := signedFleetCommand(t, privateKey, "terminal-one", "transport-command", "drain", 1, now)
	data, _ := json.Marshal(envelope)
	request := httptest.NewRequest(http.MethodPost, TransportPath, bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer separate-operator-token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || len(executor.actions) != 1 || executor.actions[0] != "drain" {
		t.Fatalf("status=%d body=%s actions=%v", response.Code, response.Body.String(), executor.actions)
	}

	unauthorized := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, TransportPath, bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(unauthorized, request)
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}
}

func TestFleetTransportRejectsAmbiguousAndOversizedJSON(t *testing.T) {
	publicKey, _ := fleetKey(t)
	online, busy := true, false
	agent := openTestAgent(t, "terminal-one", publicKey, &mockExecutor{}, &online, &busy)
	handler, _ := NewTransportHandler(TransportConfig{
		Agent: agent, BearerToken: "separate-operator-token", Now: time.Now, MaxConcurrent: 1,
	})
	for _, body := range [][]byte{
		[]byte(`{"version":1,"version":1}`),
		[]byte(strings.Repeat("x", MaxTransportBody+1)),
	} {
		request := httptest.NewRequest(http.MethodPost, TransportPath, bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer separate-operator-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestFleetTransportRejectsUnsafeBearerConfiguration(t *testing.T) {
	publicKey, _ := fleetKey(t)
	online, busy := true, false
	agent := openTestAgent(t, "terminal-one", publicKey, &mockExecutor{}, &online, &busy)
	for _, token := range []string{"short", "operator-token-with-newline\n", "operator token with space"} {
		if _, err := NewTransportHandler(TransportConfig{
			Agent: agent, BearerToken: token, Now: time.Now, MaxConcurrent: 1,
		}); err == nil {
			t.Fatalf("unsafe bearer token %q accepted", token)
		}
	}
}
