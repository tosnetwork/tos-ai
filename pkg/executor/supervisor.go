package executor

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
)

const MaxSupervisedActiveHard = 256

// BackendDriver is the narrow runtime-specific implementation beneath the
// process-local workload supervisor. Close must synchronously stop and clean
// every driver workload before it returns.
type BackendDriver interface {
	ContainerdClient
	RuntimeReadiness
	io.Closer
}

// SupervisedBackend bounds active runtime identities before a request reaches
// a future containerd driver. It creates no goroutine or admission queue:
// requests above the fixed capacity fail immediately.
type SupervisedBackend struct {
	driver BackendDriver
	slots  chan struct{}

	mutex     sync.Mutex
	active    map[string]struct{}
	isClosed  bool
	probes    sync.WaitGroup
	wait      sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func NewSupervisedBackend(
	driver BackendDriver,
	maxActive int,
) (*SupervisedBackend, error) {
	if nilBackendDriver(driver) || maxActive <= 0 ||
		maxActive > MaxSupervisedActiveHard {
		return nil, errors.New("invalid isolated backend supervisor configuration")
	}
	return &SupervisedBackend{
		driver: driver, slots: make(chan struct{}, maxActive),
		active: make(map[string]struct{}, maxActive),
	}, nil
}

func (b *SupervisedBackend) CheckReady(ctx context.Context) error {
	if b == nil || nilBackendDriver(b.driver) {
		return errors.New("invalid isolated backend supervisor")
	}
	if ctx == nil {
		return errors.New("nil isolated backend readiness context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mutex.Lock()
	if b.isClosed {
		b.mutex.Unlock()
		return errors.New("isolated backend supervisor is closed")
	}
	b.probes.Add(1)
	b.mutex.Unlock()
	defer b.probes.Done()
	err := callDriverReady(ctx, b.driver)
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if err != nil {
		return errors.New("isolated backend driver is unavailable")
	}
	return nil
}

func (b *SupervisedBackend) RunIsolated(
	ctx context.Context,
	request ContainerRequest,
	input []byte,
) (Result, error) {
	if b == nil || nilBackendDriver(b.driver) || b.slots == nil {
		return Result{}, errors.New("invalid isolated backend supervisor")
	}
	if ctx == nil {
		return Result{}, errors.New("nil isolated backend context")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := ValidateExecutionDigest(request.ExecutionDigest); err != nil {
		return Result{}, err
	}
	b.mutex.Lock()
	if b.isClosed {
		b.mutex.Unlock()
		return Result{}, errors.New("isolated backend supervisor is closed")
	}
	if _, exists := b.active[request.ExecutionDigest]; exists {
		b.mutex.Unlock()
		return Result{}, errors.New("isolated execution is already active")
	}
	select {
	case b.slots <- struct{}{}:
	default:
		b.mutex.Unlock()
		return Result{}, errors.New("isolated backend capacity is exhausted")
	}
	b.active[request.ExecutionDigest] = struct{}{}
	b.wait.Add(1)
	b.mutex.Unlock()

	defer func() {
		b.mutex.Lock()
		delete(b.active, request.ExecutionDigest)
		b.mutex.Unlock()
		<-b.slots
		b.wait.Done()
	}()
	result, err := callBackendDriver(
		ctx, b.driver, cloneContainerRequest(request), append([]byte(nil), input...),
	)
	if contextErr := ctx.Err(); contextErr != nil {
		return Result{}, contextErr
	}
	if err != nil {
		return Result{}, errors.New("isolated backend driver failed")
	}
	return result, nil
}

func (b *SupervisedBackend) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.mutex.Lock()
		b.isClosed = true
		b.mutex.Unlock()
		b.probes.Wait()
		b.closeErr = callDriverClose(b.driver)
		b.wait.Wait()
	})
	return b.closeErr
}

func cloneContainerRequest(request ContainerRequest) ContainerRequest {
	request.Entrypoint = append([]string(nil), request.Entrypoint...)
	request.AllowedHosts = append([]string(nil), request.AllowedHosts...)
	request.Environment = cloneStringsMap(request.Environment)
	return request
}

func callBackendDriver(
	ctx context.Context,
	driver BackendDriver,
	request ContainerRequest,
	input []byte,
) (result Result, err error) {
	defer func() {
		if recover() != nil {
			result = Result{}
			err = errors.New("isolated backend driver panicked")
		}
	}()
	return driver.RunIsolated(ctx, request, input)
}

func callDriverReady(ctx context.Context, driver BackendDriver) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("isolated backend readiness panicked")
		}
	}()
	return driver.CheckReady(ctx)
}

func callDriverClose(driver BackendDriver) (err error) {
	if nilBackendDriver(driver) {
		return errors.New("invalid isolated backend driver")
	}
	defer func() {
		if recover() != nil {
			err = errors.New("isolated backend close panicked")
		}
	}()
	if err := driver.Close(); err != nil {
		return errors.New("close isolated backend driver")
	}
	return nil
}

func nilBackendDriver(driver BackendDriver) bool {
	if driver == nil {
		return true
	}
	value := reflect.ValueOf(driver)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ BackendDriver = (*SupervisedBackend)(nil)
