package executor

import (
	"context"
	"errors"
	"reflect"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

// RuntimeAdapterConfig binds one operator-reviewed immutable container spec to
// one advertised Worker capability. Requests may reduce the output bound and
// provide input bytes, but cannot select an image, entrypoint, environment,
// network destination, identity, GPU access, or other sandbox property.
type RuntimeAdapterConfig struct {
	Capability airuntime.Capability
	Preflight  airuntime.Preflight
	Spec       Spec
	Executor   *PolicyExecutor
	Readiness  RuntimeReadiness
}

// RuntimeReadiness is implemented by the same fixed backend that executes the
// workload. It must perform a bounded, cancellation-aware local health check
// without creating a task or changing model/runtime state.
type RuntimeReadiness interface {
	CheckReady(context.Context) error
}

// RuntimeAdapter makes the isolation contract consumable by the ordinary
// Worker scheduler without exposing arbitrary container execution over RPC.
type RuntimeAdapter struct {
	capability airuntime.Capability
	preflight  airuntime.Preflight
	spec       Spec
	executor   *PolicyExecutor
	readiness  RuntimeReadiness
}

func NewRuntimeAdapter(config RuntimeAdapterConfig) (*RuntimeAdapter, error) {
	if err := airuntime.ValidateCapability(config.Capability); err != nil {
		return nil, errors.New("invalid isolated runtime capability")
	}
	if err := airuntime.ValidatePreflight(
		config.Capability, config.Preflight,
	); err != nil {
		return nil, errors.New("invalid isolated runtime preflight")
	}
	if config.Preflight.DigestEvidence != airuntime.BindingDeclared {
		return nil, errors.New(
			"isolated runtime adapter cannot upgrade declared model evidence",
		)
	}
	if config.Executor == nil || config.Executor.client == nil {
		return nil, errors.New("nil isolated executor")
	}
	if nilRuntimeReadiness(config.Readiness) {
		return nil, errors.New("nil isolated runtime readiness")
	}
	if err := validateRuntimeSpec(config.Capability, config.Spec); err != nil {
		return nil, err
	}
	if err := config.Executor.policy.Validate(config.Spec); err != nil {
		return nil, errors.New("isolated runtime spec violates executor policy")
	}
	return &RuntimeAdapter{
		capability: cloneRuntimeCapability(config.Capability),
		preflight:  config.Preflight,
		spec:       cloneSpec(config.Spec),
		executor:   config.Executor,
		readiness:  config.Readiness,
	}, nil
}

func (a *RuntimeAdapter) Capability() airuntime.Capability {
	if a == nil {
		return airuntime.Capability{}
	}
	return cloneRuntimeCapability(a.capability)
}

func (a *RuntimeAdapter) Preflight(
	ctx context.Context,
) (airuntime.Preflight, error) {
	if a == nil || a.executor == nil || a.executor.client == nil ||
		nilRuntimeReadiness(a.readiness) {
		return airuntime.Preflight{}, airuntime.NewError(
			airuntime.ErrorInternal, errors.New("invalid isolated runtime adapter"),
		)
	}
	if ctx == nil {
		return airuntime.Preflight{}, airuntime.NewError(
			airuntime.ErrorInvalid, errors.New("nil preflight context"),
		)
	}
	if err := ctx.Err(); err != nil {
		return airuntime.Preflight{}, runtimeContextError(err)
	}
	err := callRuntimeReadiness(ctx, a.readiness)
	if contextErr := runtimeContextStatus(ctx); contextErr != nil {
		return airuntime.Preflight{}, runtimeContextError(contextErr)
	}
	if err != nil {
		return airuntime.Preflight{}, airuntime.NewError(
			airuntime.ErrorUnavailable,
			errors.New("isolated runtime is unavailable"),
		)
	}
	return a.preflight, nil
}

func (a *RuntimeAdapter) Execute(
	ctx context.Context,
	request airuntime.Request,
) (airuntime.Response, error) {
	if a == nil || a.executor == nil || a.executor.client == nil {
		return airuntime.Response{}, airuntime.NewError(
			airuntime.ErrorInternal, errors.New("invalid isolated runtime adapter"),
		)
	}
	if ctx == nil {
		return airuntime.Response{}, airuntime.NewError(
			airuntime.ErrorInvalid, errors.New("nil execution context"),
		)
	}
	if err := ctx.Err(); err != nil {
		return airuntime.Response{}, runtimeContextError(err)
	}
	if err := airuntime.ValidateRequest(a.capability, request); err != nil {
		return airuntime.Response{}, airuntime.NewError(
			airuntime.ErrorInvalid, err,
		)
	}
	spec := cloneSpec(a.spec)
	spec.Limits.OutputBytes = request.MaxOutputBytes
	result, err := a.executor.Execute(
		ctx, request.RequestID, spec, append([]byte(nil), request.Payload...),
	)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return airuntime.Response{}, runtimeContextError(contextErr)
		}
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return airuntime.Response{}, runtimeContextError(err)
		}
		return airuntime.Response{}, airuntime.NewError(
			airuntime.ErrorInternal, errors.New("isolated execution failed"),
		)
	}
	if result.ExitCode != 0 {
		return airuntime.Response{}, airuntime.NewError(
			airuntime.ErrorRemote, errors.New("isolated workload rejected request"),
		)
	}
	return airuntime.Response{
		Output: append([]byte(nil), result.Output...),
		Usage: airuntime.Usage{
			InputBytes:      uint64(len(request.Payload)),
			OutputBytes:     uint64(len(result.Output)),
			ExecutionMillis: durationMilliseconds(result.Usage.Duration),
		},
		ModelRevision:   a.capability.ModelDigest,
		RuntimeRevision: a.capability.RuntimeRevision,
	}, nil
}

func validateRuntimeSpec(
	capability airuntime.Capability,
	spec Spec,
) error {
	if spec.Limits.MemoryBytes != capability.Admission.RAMBytes ||
		spec.Limits.ExecutionTime != capability.Admission.ExecutionTime ||
		spec.Limits.OutputBytes != capability.MaxOutputBytes {
		return errors.New("isolated runtime spec does not match advertised limits")
	}
	if capability.Admission.VRAMBytes > 0 {
		if !spec.AllowGPU || spec.Limits.GPUDeviceCount == 0 {
			return errors.New("isolated GPU capability lacks fixed GPU access")
		}
	} else if spec.AllowGPU || spec.Limits.GPUDeviceCount != 0 {
		return errors.New("isolated CPU capability enables GPU access")
	}
	if spec.Limits.CPUMillis == 0 || spec.Limits.DiskBytes == 0 ||
		spec.Limits.PIDs == 0 {
		return errors.New("isolated runtime spec has incomplete resource limits")
	}
	return nil
}

func cloneRuntimeCapability(
	capability airuntime.Capability,
) airuntime.Capability {
	capability.AcceptedPriorities = append(
		[]airuntime.Priority(nil), capability.AcceptedPriorities...,
	)
	return capability
}

func cloneSpec(spec Spec) Spec {
	spec.Entrypoint = append([]string(nil), spec.Entrypoint...)
	spec.AllowedHosts = append([]string(nil), spec.AllowedHosts...)
	spec.HostMounts = append([]Mount(nil), spec.HostMounts...)
	spec.Environment = cloneStringsMap(spec.Environment)
	return spec
}

func runtimeContextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return airuntime.NewError(airuntime.ErrorCanceled, context.Canceled)
	}
	return airuntime.NewError(airuntime.ErrorTimeout, context.DeadlineExceeded)
}

func runtimeContextStatus(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func durationMilliseconds(value time.Duration) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value / time.Millisecond)
}

func nilRuntimeReadiness(readiness RuntimeReadiness) bool {
	if readiness == nil {
		return true
	}
	value := reflect.ValueOf(readiness)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func callRuntimeReadiness(
	ctx context.Context,
	readiness RuntimeReadiness,
) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("isolated runtime readiness panicked")
		}
	}()
	return readiness.CheckReady(ctx)
}

var _ airuntime.Adapter = (*RuntimeAdapter)(nil)
