package probe

import "testing"

func TestLocalGateMockTier1TelemetryMatrix(t *testing.T) {
	healthy := fakeGPU{
		name: "NVIDIA RTX 4090 MOCK", total: 24 << 30, used: 4 << 30,
		major: 8, minor: 9, temperature: 62,
		power: 280_000, limit: 450_000,
	}
	report := CollectNVIDIA(&fakeNVIDIA{count: 1, devices: []fakeGPU{healthy}})
	if report.Status != "available" || report.Evidence != EvidenceLocallyObserved ||
		len(report.Devices) != 1 || report.Devices[0].VRAMBytes != 24<<30 ||
		report.Devices[0].PowerState != "normal" {
		t.Fatalf("healthy simulated Tier 1 report=%#v", report)
	}
	host, err := Collect(&fakeNVIDIA{count: 1, devices: []fakeGPU{healthy}})
	if err != nil || ValidateReport(host) != nil {
		t.Fatalf("healthy simulated host report=%#v err=%v", host, err)
	}

	overPower := healthy
	overPower.power = overPower.limit + 1
	report = CollectNVIDIA(&fakeNVIDIA{count: 1, devices: []fakeGPU{overPower}})
	if report.Status != "degraded" || report.Devices[0].PowerState != "above-limit" {
		t.Fatalf("over-power simulation did not degrade: %#v", report)
	}
	overTemperature := healthy
	overTemperature.temperature = maxTemperatureC + 1
	report = CollectNVIDIA(&fakeNVIDIA{count: 1, devices: []fakeGPU{overTemperature}})
	if report.Status != "degraded" || report.Devices[0].TemperatureC != nil {
		t.Fatalf("over-temperature simulation did not fail closed: %#v", report)
	}
	exhausted := healthy
	exhausted.used = exhausted.total + 1
	report = CollectNVIDIA(&fakeNVIDIA{count: 1, devices: []fakeGPU{exhausted}})
	if report.Status != "degraded" || report.Devices[0].VRAMBytes != 0 {
		t.Fatalf("invalid VRAM simulation did not fail closed: %#v", report)
	}

	recovered := CollectNVIDIA(&fakeNVIDIA{count: 1, devices: []fakeGPU{healthy}})
	if recovered.Status != "available" || recovered.Devices[0].PowerState != "normal" {
		t.Fatalf("simulated telemetry did not recover: %#v", recovered)
	}
}
