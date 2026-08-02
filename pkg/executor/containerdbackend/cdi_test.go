package containerdbackend

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
)

func TestCDIInjectorUsesOnlyFixedRegistryDevice(t *testing.T) {
	directory := t.TempDir()
	specification := `{
  "cdiVersion":"0.6.0",
  "kind":"tos.test/gpu",
  "devices":[{
    "name":"mock0",
    "containerEdits":{
      "env":["TOS_MOCK_GPU=injected"],
      "deviceNodes":[{"path":"/dev/null","type":"c","major":1,"minor":3}]
    }
  }]
}`
	if err := os.WriteFile(
		filepath.Join(directory, "tos-test-gpu.json"),
		[]byte(specification), 0o600,
	); err != nil {
		t.Fatal(err)
	}
	injector, err := newCDIInjector([]string{directory})
	if err != nil {
		t.Fatal(err)
	}
	ctx := namespaces.WithNamespace(context.Background(), "tos-ai-test")
	spec, err := oci.GenerateSpec(
		ctx, nil, &containers.Container{ID: "cdi-test"},
		injector.specOpt([]string{"tos.test/gpu=mock0"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	foundEnvironment := false
	for _, value := range spec.Process.Env {
		if value == "TOS_MOCK_GPU=injected" {
			foundEnvironment = true
		}
	}
	foundDevice := false
	for _, device := range spec.Linux.Devices {
		if device.Path == "/dev/null" && device.Type == "c" &&
			device.Major == 1 && device.Minor == 3 {
			foundDevice = true
		}
	}
	if !foundEnvironment || !foundDevice {
		t.Fatalf("CDI edit missing: env=%v devices=%#v", spec.Process.Env, spec.Linux.Devices)
	}
	if _, err := oci.GenerateSpec(
		ctx, nil, &containers.Container{ID: "cdi-missing"},
		injector.specOpt([]string{"tos.test/gpu=missing"}),
	); err == nil {
		t.Fatal("unknown CDI device was accepted")
	}
}

func TestCDIInjectorRejectsInvalidConstructionAndNilUse(t *testing.T) {
	if injector, err := newCDIInjector(nil); err == nil || injector != nil {
		t.Fatal("empty CDI registry configuration accepted")
	}
	ctx := namespaces.WithNamespace(context.Background(), "tos-ai-test")
	if _, err := oci.GenerateSpec(
		ctx, nil, &containers.Container{ID: "nil-cdi"},
		(*cdiInjector)(nil).specOpt([]string{"tos.test/gpu=mock0"}),
	); err == nil {
		t.Fatal("nil CDI injector accepted devices")
	}
}
