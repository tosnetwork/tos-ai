// Package gpuisolation provides the exclusive GPU-device lease boundary used
// in front of a reviewed container backend. Device aliases are fixed by the
// operator and never selected by a remote request. The backend remains
// responsible for translating aliases into an audited OCI device injection.
package gpuisolation

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/tosnetwork/tos-ai/internal/nilcheck"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const MaxDevices = 64

type Backend interface {
	RunIsolatedOnDevices(context.Context, executor.ContainerRequest, []byte, []string) (executor.Result, error)
}

type Client struct {
	mu      sync.Mutex
	backend Backend
	devices []string
	leased  map[string]string
}

func New(aliases []string, backend Backend) (*Client, error) {
	if len(aliases) == 0 || len(aliases) > MaxDevices || nilcheck.IsNil(backend) {
		return nil, errors.New("invalid GPU isolation configuration")
	}
	devices := append([]string(nil), aliases...)
	sort.Strings(devices)
	for index, alias := range devices {
		if !validAlias(alias) || index > 0 && devices[index-1] == alias {
			return nil, errors.New("invalid GPU device alias")
		}
	}
	return &Client{backend: backend, devices: devices, leased: make(map[string]string, len(devices))}, nil
}

func (c *Client) RunIsolated(ctx context.Context, request executor.ContainerRequest, input []byte) (result executor.Result, resultErr error) {
	if c == nil || ctx == nil || nilcheck.IsNil(c.backend) || executor.ValidateExecutionDigest(request.ExecutionDigest) != nil {
		return executor.Result{}, errors.New("invalid GPU isolation request")
	}
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	count := int(request.Limits.GPUDeviceCount)
	if request.AllowGPU != (count > 0) {
		return executor.Result{}, errors.New("GPU isolation request is inconsistent")
	}
	devices, err := c.acquire(request.ExecutionDigest, count)
	if err != nil {
		return executor.Result{}, err
	}
	defer c.release(request.ExecutionDigest, devices)
	defer func() {
		if recover() != nil {
			result = executor.Result{}
			resultErr = errors.New("GPU isolation backend failed")
		}
	}()
	result, err = c.backend.RunIsolatedOnDevices(ctx, request, append([]byte(nil), input...), append([]string(nil), devices...))
	if err != nil {
		return executor.Result{}, errors.New("GPU isolation backend failed")
	}
	// A backend may return a late success after the caller has cancelled. Never
	// publish that result as successful work.
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	return result, nil
}

func (c *Client) Available() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.devices) - len(c.leased)
}

func (c *Client) acquire(owner string, count int) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if count < 0 || count > len(c.devices)-len(c.leased) {
		return nil, errors.New("GPU device capacity exhausted")
	}
	if count == 0 {
		return nil, nil
	}
	for _, currentOwner := range c.leased {
		if currentOwner == owner {
			return nil, errors.New("execution already owns GPU devices")
		}
	}
	devices := make([]string, 0, count)
	for _, alias := range c.devices {
		if _, busy := c.leased[alias]; !busy {
			c.leased[alias] = owner
			devices = append(devices, alias)
			if len(devices) == count {
				break
			}
		}
	}
	return devices, nil
}

func (c *Client) release(owner string, devices []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, alias := range devices {
		if c.leased[alias] == owner {
			delete(c.leased, alias)
		}
	}
}

func validAlias(value string) bool {
	if len(value) < 3 || len(value) > 64 || strings.Contains(value, "..") {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z') && !(character >= '0' && character <= '9') && character != '-' {
			return false
		}
	}
	return true
}

var _ executor.ContainerdClient = (*Client)(nil)
