package probe

import "testing"

func TestCollectHost(t *testing.T) {
	host, err := CollectHost()
	if err != nil {
		t.Fatal(err)
	}
	if host.OS == "" || host.Architecture == "" || host.LogicalCPUs < 1 ||
		host.Evidence != "locally-observed" {
		t.Fatalf("invalid host probe: %#v", host)
	}
}
