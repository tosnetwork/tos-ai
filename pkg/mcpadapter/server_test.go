package mcpadapter

import "testing"

func TestOfficialStreamableHTTPBinding(t *testing.T) {
	adapter, err := New(&gateFake{}, &runnerFake{}, locatorFake{})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewStreamableHTTPHandler(adapter)
	if err != nil || handler == nil {
		t.Fatalf("MCP streamable HTTP binding failed: %v", err)
	}
}
