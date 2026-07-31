package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	airuntime "github.com/tosnetwork/tos-ai/pkg/runtime"
)

func TestPriorityAndFIFOOrdering(t *testing.T) {
	scheduler, _ := New(Config{Workers: 1, MaxQueue: 4})
	var mu sync.Mutex
	var order []string
	submit := func(id string, priority airuntime.Priority) <-chan Result {
		result, err := scheduler.Submit(Item{
			ID:       id,
			Priority: priority,
			Deadline: time.Now().Add(time.Minute),
			Work: func(context.Context) (airuntime.Response, error) {
				mu.Lock()
				order = append(order, id)
				mu.Unlock()
				return airuntime.Response{}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	background := submit("background", airuntime.PriorityBackground)
	external := submit("external", airuntime.PriorityExternalService)
	local := submit("local", airuntime.PriorityLocalAsync)
	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	<-background
	<-external
	<-local
	if got := order; len(got) != 3 || got[0] != "local" || got[1] != "external" || got[2] != "background" {
		t.Fatalf("execution order = %v", got)
	}
	_ = scheduler.Shutdown(context.Background())
}

func TestQueueBoundAndCancellation(t *testing.T) {
	scheduler, _ := New(Config{Workers: 1, MaxQueue: 1})
	result, err := scheduler.Submit(Item{
		ID:       "one",
		Priority: airuntime.PriorityExternalService,
		Deadline: time.Now().Add(time.Minute),
		Work: func(context.Context) (airuntime.Response, error) {
			return airuntime.Response{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Submit(Item{
		ID:       "two",
		Priority: airuntime.PriorityExternalService,
		Deadline: time.Now().Add(time.Minute),
		Work:     func(context.Context) (airuntime.Response, error) { return airuntime.Response{}, nil },
	}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue overflow error = %v", err)
	}
	if !scheduler.Cancel("one") {
		t.Fatal("cancel rejected")
	}
	if outcome := <-result; !errors.Is(outcome.Err, ErrCanceled) {
		t.Fatalf("cancel outcome = %v", outcome.Err)
	}
	_ = scheduler.Shutdown(context.Background())
}

func TestOwnerReservedWorkersRejectInvalidConfiguration(t *testing.T) {
	for _, config := range []Config{
		{Workers: 2, MaxQueue: 1, OwnerReservedWorkers: -1},
		{Workers: 2, MaxQueue: 1, OwnerReservedWorkers: 2},
		{Workers: 2, MaxQueue: 1, OwnerReservedWorkers: 3},
	} {
		if _, err := New(config); err == nil {
			t.Fatalf("invalid owner worker reserve accepted: %#v", config)
		}
	}
}

func TestOwnerReservedWorkerRemainsAvailableUnderExternalSaturation(t *testing.T) {
	scheduler, err := New(Config{
		Workers: 2, MaxQueue: 4, OwnerReservedWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scheduler.Shutdown(context.Background()) })

	externalStarted := make(chan string, 2)
	externalRelease := make(chan struct{})
	submitBlocking := func(
		id string,
		priority airuntime.Priority,
		started chan<- string,
		release <-chan struct{},
	) <-chan Result {
		t.Helper()
		result, submitErr := scheduler.Submit(Item{
			ID: id, Priority: priority, Deadline: time.Now().Add(time.Minute),
			Work: func(ctx context.Context) (airuntime.Response, error) {
				started <- id
				select {
				case <-release:
					return airuntime.Response{}, nil
				case <-ctx.Done():
					return airuntime.Response{}, ctx.Err()
				}
			},
		})
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		return result
	}
	first := submitBlocking(
		"external-one", airuntime.PriorityExternalService,
		externalStarted, externalRelease,
	)
	second := submitBlocking(
		"external-two", airuntime.PriorityExternalService,
		externalStarted, externalRelease,
	)
	select {
	case <-externalStarted:
	case <-time.After(time.Second):
		t.Fatal("first external task did not start")
	}
	select {
	case id := <-externalStarted:
		t.Fatalf("external task %q consumed the owner-reserved worker", id)
	case <-time.After(50 * time.Millisecond):
	}

	localStarted := make(chan string, 1)
	localRelease := make(chan struct{})
	local := submitBlocking(
		"local-owner", airuntime.PriorityLocalAsync, localStarted, localRelease,
	)
	select {
	case <-localStarted:
	case <-time.After(time.Second):
		t.Fatal("owner task was starved by external saturation")
	}
	close(localRelease)
	if outcome := <-local; outcome.Err != nil {
		t.Fatalf("owner task outcome=%v", outcome.Err)
	}
	close(externalRelease)
	if outcome := <-first; outcome.Err != nil {
		t.Fatalf("first external outcome=%v", outcome.Err)
	}
	if outcome := <-second; outcome.Err != nil {
		t.Fatalf("second external outcome=%v", outcome.Err)
	}
}

func TestOwnerTasksCanUseEveryWorker(t *testing.T) {
	const workers = 3
	scheduler, err := New(Config{
		Workers: workers, MaxQueue: workers, OwnerReservedWorkers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = scheduler.Shutdown(context.Background()) })
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	results := make([]<-chan Result, 0, workers)
	for index := 0; index < workers; index++ {
		result, submitErr := scheduler.Submit(Item{
			ID:       string(rune('a' + index)),
			Priority: airuntime.PriorityLocalAsync,
			Deadline: time.Now().Add(time.Minute),
			Work: func(ctx context.Context) (airuntime.Response, error) {
				started <- struct{}{}
				select {
				case <-release:
					return airuntime.Response{}, nil
				case <-ctx.Done():
					return airuntime.Response{}, ctx.Err()
				}
			},
		})
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		results = append(results, result)
	}
	for range workers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("owner work did not use all scheduler workers")
		}
	}
	close(release)
	for _, result := range results {
		if outcome := <-result; outcome.Err != nil {
			t.Fatal(outcome.Err)
		}
	}
}

func TestOwnerReservedQueueCancelsAndShutsDown(t *testing.T) {
	scheduler, err := New(Config{
		Workers: 2, MaxQueue: 4, OwnerReservedWorkers: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Start(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	running, err := scheduler.Submit(Item{
		ID:       "external-running",
		Priority: airuntime.PriorityExternalService,
		Deadline: time.Now().Add(time.Minute),
		Work: func(ctx context.Context) (airuntime.Response, error) {
			close(started)
			<-ctx.Done()
			return airuntime.Response{}, ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	pending, err := scheduler.Submit(Item{
		ID:       "external-pending",
		Priority: airuntime.PriorityExternalService,
		Deadline: time.Now().Add(time.Minute),
		Work: func(context.Context) (airuntime.Response, error) {
			return airuntime.Response{}, errors.New(
				"pending external work used the owner-reserved worker",
			)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !scheduler.Cancel("external-pending") {
		t.Fatal("pending external cancellation was rejected")
	}
	if outcome := <-pending; !errors.Is(outcome.Err, ErrCanceled) {
		t.Fatalf("pending cancellation outcome=%v", outcome.Err)
	}
	stopped, err := scheduler.Submit(Item{
		ID:       "external-stopped",
		Priority: airuntime.PriorityExternalService,
		Deadline: time.Now().Add(time.Minute),
		Work: func(context.Context) (airuntime.Response, error) {
			return airuntime.Response{}, errors.New("shutdown-pending work executed")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := scheduler.Shutdown(shutdownContext); err != nil {
		t.Fatal(err)
	}
	if outcome := <-stopped; !errors.Is(outcome.Err, ErrStopped) {
		t.Fatalf("shutdown pending outcome=%v", outcome.Err)
	}
	if outcome := <-running; !errors.Is(outcome.Err, context.Canceled) {
		t.Fatalf("shutdown running outcome=%v", outcome.Err)
	}
}
