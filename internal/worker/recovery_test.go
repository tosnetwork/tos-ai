package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tosnetwork/tos-ai/pkg/adapters/mock"
	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

func TestServiceStartupFailsInterruptedTasksAndRemovesExpiredTasks(t *testing.T) {
	createdAt := time.Unix(1_800_000_000, 0).UTC()
	startupAt := createdAt.Add(3 * time.Minute)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "worker-tasks.db")
	storeConfig := localrpc.DefaultWorkerTaskStoreConfig(path)
	storeConfig.MaxTasks = 4
	storeConfig.MaxInvocationDuration = time.Hour
	storeConfig.AllowedPriorities = []edgev1.Priority{
		edgev1.Priority_PRIORITY_LOCAL_ASYNC,
		edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		edgev1.Priority_PRIORITY_BACKGROUND,
	}
	first, err := localrpc.OpenWorkerTaskStore(storeConfig)
	if err != nil {
		t.Fatal(err)
	}
	claim := func(suffix string, retainUntil time.Time) localrpc.StoredWorkerTask {
		t.Helper()
		request := bindTestInvocation(t, &edgev1.InvokeRequest{
			RequestId: "request-" + suffix, QuoteId: "quote-" + suffix,
			ServiceId: "tos.ai.mock", Operation: "generate",
			Model: "deterministic-echo", Payload: []byte("input-" + suffix),
			MaxOutputBytes:        64,
			DeadlineUnixMillis:    createdAt.Add(time.Minute).UnixMilli(),
			RetainUntilUnixMillis: retainUntil.UnixMilli(),
			Priority:              edgev1.Priority_PRIORITY_EXTERNAL_SERVICE,
		})
		task, disposition, err := first.ClaimTask(request, createdAt)
		if err != nil || disposition != localrpc.TaskClaimed {
			t.Fatalf("claim %s disposition=%q err=%v", suffix, disposition, err)
		}
		return task
	}
	accepted := claim("restart-accepted", createdAt.Add(time.Hour))
	running := claim("restart-running", createdAt.Add(time.Hour))
	if _, _, err := first.MarkTaskRunning(
		mustStoredIdentity(t, running), createdAt.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	expired := claim("restart-expired", createdAt.Add(2*time.Minute))
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := localrpc.OpenWorkerTaskStore(storeConfig)
	if err != nil {
		t.Fatal(err)
	}
	config := testServiceConfig(t)
	if err := config.TaskStore.Close(); err != nil {
		t.Fatal(err)
	}
	config.TaskStore = second
	config.Now = func() time.Time { return startupAt }
	taskScheduler, admissionController := newTestDependencies(t, 4)
	service, err := NewService(
		config, taskScheduler, admissionController,
		[]airuntime.Adapter{mock.New(0)},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
	})
	if service.startupRecovery.ExpiredRemoved != 1 ||
		service.startupRecovery.Interrupted != 2 {
		t.Fatalf("startup recovery=%#v", service.startupRecovery)
	}
	for _, task := range []localrpc.StoredWorkerTask{accepted, running} {
		response, err := second.GetTask(storedTaskLookup(task), startupAt)
		if err != nil ||
			response.Status != edgev1.TaskStatus_TASK_STATUS_FAILED ||
			response.ErrorCode != "RUNTIME_FAILED" || response.Result != nil {
			t.Fatalf("interrupted task response=%v err=%v", response, err)
		}
	}
	response, err := second.GetTask(storedTaskLookup(expired), startupAt)
	if err != nil || response.Status != edgev1.TaskStatus_TASK_STATUS_NOT_FOUND {
		t.Fatalf("expired task response=%v err=%v", response, err)
	}
	stats, err := second.Stats()
	if err != nil || stats.Tasks != 2 || stats.Capacity != 4 || stats.Available != 2 {
		t.Fatalf("post-recovery stats=%#v err=%v", stats, err)
	}
	_, metricsBody := scrapeMetrics(t, NewOperationalMetrics().Handler(service))
	for _, metric := range []string{
		"tos_ai_worker_startup_interrupted_tasks_failed_total 2\n",
		"tos_ai_worker_startup_expired_tasks_removed_total 1\n",
	} {
		if !strings.Contains(metricsBody, metric) {
			t.Fatalf("startup metrics missing %q", metric)
		}
	}
	repeated, err := prepareTaskStoreForStartup(second, startupAt)
	if err != nil || repeated != (startupTaskRecovery{}) {
		t.Fatalf("repeated startup recovery=%#v err=%v", repeated, err)
	}
}

func mustStoredIdentity(
	t *testing.T,
	task localrpc.StoredWorkerTask,
) localrpc.WorkerTaskIdentity {
	t.Helper()
	identity, err := task.Identity()
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func storedTaskLookup(task localrpc.StoredWorkerTask) *edgev1.GetTaskRequest {
	return &edgev1.GetTaskRequest{
		RequestId: task.Request.RequestId, TaskId: task.Request.TaskId,
		RequestDigest:         task.Request.RequestDigest,
		RetainUntilUnixMillis: task.Request.RetainUntilUnixMillis,
	}
}
