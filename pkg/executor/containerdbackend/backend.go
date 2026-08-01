package containerdbackend

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tosnetwork/tos-ai/internal/dirlock"
	"github.com/tosnetwork/tos-ai/pkg/executor"
)

const (
	DefaultOperationTimeout = 10 * time.Second
	DefaultCleanupTimeout   = 30 * time.Second
	MaxOperationTimeout     = time.Minute
	MaxCleanupTimeout       = 5 * time.Minute
	MaxInputBytesHard       = 16 << 20
	MaxOutputBytesHard      = 16 << 20
	MaxArgumentsHard        = 64
	MaxEnvironmentHard      = 64
	MaxStringBytesHard      = 4096
	MaxMemoryBytesHard      = uint64(1 << 40)
	MaxDiskBytesHard        = uint64(16 << 40)
	MaxPIDsHard             = uint32(1 << 20)
	MaxExecutionTimeHard    = time.Hour
	MaxCPUMillisHard        = uint64(24 * time.Hour / time.Millisecond)
	minimumTmpfsBytes       = 3 * 4096
	ownershipLockName       = ".containerd-backend.lock"
)

var runtimeIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,75}$`)

type Config struct {
	SocketPath       string
	Namespace        string
	Snapshotter      string
	Runtime          string
	FIFODir          string
	MaxActive        int
	OperationTimeout time.Duration
	CleanupTimeout   time.Duration
	PermitGPU        bool
	PermitNetwork    bool
	PolicyLimits     executor.Limits
	ImageReference   string
	ImageDigest      string
}

type engine interface {
	CheckReady(context.Context) error
	CheckResidue(context.Context) error
	Run(context.Context, string, executor.ContainerRequest, []byte) (executor.Result, error)
	Close() error
}

type activeRun struct {
	cancel context.CancelFunc
}

// Backend is a bounded lifecycle boundary around one private containerd
// namespace. It owns no queue and starts no goroutine of its own.
type Backend struct {
	engine    engine
	ownership *dirlock.Lock
	maximum   int

	mutex     sync.Mutex
	active    map[string]activeRun
	closed    bool
	probes    sync.WaitGroup
	runs      sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

func Open(ctx context.Context, config Config) (*Backend, error) {
	if ctx == nil {
		return nil, errors.New("containerd backend context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config = withConfigDefaults(config)
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := validatePrivateSocket(config.SocketPath); err != nil {
		return nil, err
	}
	if err := ensurePrivateDirectory(config.FIFODir); err != nil {
		return nil, err
	}
	ownership, err := dirlock.Acquire(config.FIFODir, ownershipLockName)
	if err != nil {
		return nil, errors.New("containerd backend namespace is already owned")
	}
	if err := validateFIFODir(config.FIFODir); err != nil {
		_ = ownership.Close()
		return nil, err
	}
	runtimeEngine, err := newSDKEngine(config)
	if err != nil {
		_ = ownership.Close()
		return nil, errors.New("open containerd backend")
	}
	backend, err := newBackend(ctx, runtimeEngine, ownership, config.MaxActive)
	if err != nil {
		_ = runtimeEngine.Close()
		_ = ownership.Close()
		return nil, err
	}
	return backend, nil
}

func newBackend(
	ctx context.Context,
	runtimeEngine engine,
	ownership *dirlock.Lock,
	maximum int,
) (*Backend, error) {
	if ctx == nil || runtimeEngine == nil || maximum <= 0 ||
		maximum > executor.MaxSupervisedActiveHard {
		return nil, errors.New("invalid containerd backend")
	}
	if err := runtimeEngine.CheckReady(ctx); err != nil {
		return nil, errors.New("containerd backend is unavailable")
	}
	if err := runtimeEngine.CheckResidue(ctx); err != nil {
		return nil, errors.New("containerd backend namespace is not clean")
	}
	return &Backend{
		engine: runtimeEngine, ownership: ownership, maximum: maximum,
		active: make(map[string]activeRun, maximum),
	}, nil
}

func (b *Backend) CheckReady(ctx context.Context) error {
	if b == nil || b.engine == nil || ctx == nil {
		return errors.New("invalid containerd backend readiness request")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mutex.Lock()
	if b.closed {
		b.mutex.Unlock()
		return errors.New("containerd backend is closed")
	}
	b.probes.Add(1)
	b.mutex.Unlock()
	defer b.probes.Done()
	if err := callReady(ctx, b.engine); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return errors.New("containerd backend is unavailable")
	}
	return nil
}

func (b *Backend) RunIsolated(
	ctx context.Context,
	request executor.ContainerRequest,
	input []byte,
) (executor.Result, error) {
	if b == nil || b.engine == nil || ctx == nil {
		return executor.Result{}, errors.New("invalid containerd backend request")
	}
	if err := ctx.Err(); err != nil {
		return executor.Result{}, err
	}
	if err := validateRequest(request, input); err != nil {
		return executor.Result{}, err
	}
	runContext, cancel := context.WithCancel(ctx)
	b.mutex.Lock()
	if b.closed {
		b.mutex.Unlock()
		cancel()
		return executor.Result{}, errors.New("containerd backend is closed")
	}
	if len(b.active) >= b.maximum {
		b.mutex.Unlock()
		cancel()
		return executor.Result{}, errors.New("containerd backend capacity is exhausted")
	}
	if _, exists := b.active[request.ExecutionDigest]; exists {
		b.mutex.Unlock()
		cancel()
		return executor.Result{}, errors.New("containerd execution is already active")
	}
	b.active[request.ExecutionDigest] = activeRun{cancel: cancel}
	b.runs.Add(1)
	b.mutex.Unlock()
	defer func() {
		cancel()
		b.mutex.Lock()
		delete(b.active, request.ExecutionDigest)
		b.mutex.Unlock()
		b.runs.Done()
	}()

	result, err := callRun(
		runContext, b.engine, runtimeID(request.ExecutionDigest),
		cloneRequest(request), append([]byte(nil), input...),
	)
	if contextErr := ctx.Err(); contextErr != nil {
		return executor.Result{}, contextErr
	}
	if err != nil {
		return executor.Result{}, errors.New("containerd execution failed")
	}
	result.Output = append([]byte(nil), result.Output...)
	return result, nil
}

func (b *Backend) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		b.mutex.Lock()
		b.closed = true
		cancels := make([]context.CancelFunc, 0, len(b.active))
		for _, active := range b.active {
			cancels = append(cancels, active.cancel)
		}
		b.mutex.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		b.probes.Wait()
		b.runs.Wait()
		var engineErr error
		if b.engine != nil {
			engineErr = callClose(b.engine)
		}
		var ownershipErr error
		if b.ownership != nil {
			ownershipErr = b.ownership.Close()
		}
		b.closeErr = errors.Join(engineErr, ownershipErr)
	})
	return b.closeErr
}

func withConfigDefaults(config Config) Config {
	if config.OperationTimeout == 0 {
		config.OperationTimeout = DefaultOperationTimeout
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = DefaultCleanupTimeout
	}
	return config
}

func validateConfig(config Config) error {
	if !filepath.IsAbs(config.SocketPath) ||
		filepath.Clean(config.SocketPath) != config.SocketPath ||
		!filepath.IsAbs(config.FIFODir) ||
		filepath.Clean(config.FIFODir) != config.FIFODir ||
		!runtimeIdentifier.MatchString(config.Namespace) ||
		!runtimeIdentifier.MatchString(config.Snapshotter) ||
		!runtimeIdentifier.MatchString(config.Runtime) ||
		config.MaxActive <= 0 || config.MaxActive > executor.MaxSupervisedActiveHard ||
		config.OperationTimeout <= 0 || config.OperationTimeout > MaxOperationTimeout ||
		config.CleanupTimeout <= 0 || config.CleanupTimeout > MaxCleanupTimeout {
		return errors.New("invalid containerd backend configuration")
	}
	if config.Snapshotter != "overlayfs" || config.Runtime != "io.containerd.runc.v2" {
		return errors.New("unsupported containerd isolation backend")
	}
	if config.PermitGPU || config.PermitNetwork ||
		validateDriverLimits(config.PolicyLimits) != nil ||
		executor.ValidateExecutionDigest(config.ImageDigest) != nil ||
		len(config.ImageReference) == 0 ||
		len(config.ImageReference) > MaxStringBytesHard ||
		strings.IndexByte(config.ImageReference, 0) >= 0 ||
		!strings.HasSuffix(config.ImageReference, "@"+config.ImageDigest) {
		return errors.New("unsupported containerd isolation policy")
	}
	return nil
}

func validatePrivateSocket(path string) error {
	parent, err := os.Lstat(filepath.Dir(path))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 ||
		parent.Mode().Perm() != 0o700 || !ownedByCurrentUser(parent) {
		return errors.New("containerd socket directory is not private")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 ||
		info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) {
		return errors.New("containerd socket is not private")
	}
	return nil
}

func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return errors.New("create containerd FIFO directory")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return errors.New("containerd FIFO directory is not private")
	}
	return nil
}

func validateFIFODir(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errors.New("inspect containerd FIFO directory")
	}
	defer directory.Close()
	names, err := directory.Readdirnames(executor.MaxSupervisedActiveHard + 2)
	if err != nil && !errors.Is(err, io.EOF) {
		return errors.New("inspect containerd FIFO directory")
	}
	for _, name := range names {
		if name != ownershipLockName {
			return errors.New("containerd FIFO residue is present")
		}
	}
	return nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func runtimeID(digest string) string {
	return digest[len("sha256:"):]
}

func cloneRequest(request executor.ContainerRequest) executor.ContainerRequest {
	request.Entrypoint = append([]string(nil), request.Entrypoint...)
	request.AllowedHosts = append([]string(nil), request.AllowedHosts...)
	if request.Environment != nil {
		cloned := make(map[string]string, len(request.Environment))
		for key, value := range request.Environment {
			cloned[key] = value
		}
		request.Environment = cloned
	}
	return request
}

func callReady(ctx context.Context, runtimeEngine engine) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("containerd readiness panicked")
		}
	}()
	return runtimeEngine.CheckReady(ctx)
}

func callRun(
	ctx context.Context,
	runtimeEngine engine,
	id string,
	request executor.ContainerRequest,
	input []byte,
) (result executor.Result, err error) {
	defer func() {
		if recover() != nil {
			result = executor.Result{}
			err = errors.New("containerd execution panicked")
		}
	}()
	return runtimeEngine.Run(ctx, id, request, input)
}

func callClose(runtimeEngine engine) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("containerd close panicked")
		}
	}()
	if err := runtimeEngine.Close(); err != nil {
		return errors.New("close containerd runtime")
	}
	return nil
}

var _ executor.BackendDriver = (*Backend)(nil)
