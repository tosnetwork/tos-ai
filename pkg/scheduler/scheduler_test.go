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
