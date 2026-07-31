package main

import (
	"testing"

	"github.com/tosnetwork/tos-ai/pkg/admission"
	"github.com/tosnetwork/tos-ai/pkg/probe"
)

func TestDefaultAdmissionConfigSupportsNoGPUAndHasHardBounds(t *testing.T) {
	report := probe.Report{
		Host:   probe.Host{MemoryBytes: 16 << 30},
		NVIDIA: probe.NVIDIAReport{Status: "unavailable"},
	}
	config, err := defaultAdmissionConfig(report, 2, 8)
	if err != nil {
		t.Fatal(err)
	}
	if config.Capacity.VRAMBytes != 0 || config.MaxConcurrent != 2 ||
		config.OwnerReserved.RAMBytes == 0 {
		t.Fatalf("unexpected config: %#v", config)
	}
	if _, err := admission.New(config); err != nil {
		t.Fatal(err)
	}
	if _, err := defaultAdmissionConfig(report, admission.MaxConcurrentHard+1, 8); err == nil {
		t.Fatal("worker hard limit accepted")
	}
}

func TestDefaultAdmissionConfigRejectsInsufficientRAM(t *testing.T) {
	_, err := defaultAdmissionConfig(probe.Report{
		Host: probe.Host{MemoryBytes: 32 << 20},
	}, 1, 1)
	if err == nil {
		t.Fatal("insufficient RAM accepted")
	}
}
