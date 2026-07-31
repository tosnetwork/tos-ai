package runtimehttp

import (
	"net/http"
	"testing"
	"time"
)

func testConfig(endpoint string) Config {
	return Config{
		BaseURL: endpoint, Timeout: time.Second, ConnectTimeout: time.Second,
		MaxConnections: 2, MaxResponseHeaderBytes: 4096,
	}
}

func TestBuildRejectsRemotePlaintextAndRedirects(t *testing.T) {
	if _, _, err := Build(testConfig("http://runtime.example")); err == nil {
		t.Fatal("remote plaintext hostname accepted")
	}
	config := testConfig("http://10.0.0.5")
	config.AllowedPlaintextCIDRs = []string{"10.0.0.0/24"}
	base, client, err := Build(config)
	if err != nil {
		t.Fatal(err)
	}
	if Endpoint(base, "/v1/test") != "http://10.0.0.5/v1/test" {
		t.Fatalf("endpoint=%q", Endpoint(base, "/v1/test"))
	}
	if err := client.CheckRedirect(&http.Request{}, nil); err == nil {
		t.Fatal("redirect accepted")
	}
	config.MaxConnections = MaxConnectionsHard + 1
	if _, _, err := Build(config); err == nil {
		t.Fatal("connection hard limit accepted")
	}
}
