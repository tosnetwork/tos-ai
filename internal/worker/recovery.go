package worker

import (
	"errors"
	"time"

	edgev1 "github.com/tosnetwork/tos-protocol/gen/tos/edge/v1"
	"github.com/tosnetwork/tos-protocol/pkg/localrpc"
)

type startupTaskRecovery struct {
	ExpiredRemoved uint64
	Interrupted    uint64
}

// prepareTaskStoreForStartup runs before the private listener can accept an
// Invoke. Current runtime adapters are synchronous and expose no durable job
// handle, so an interrupted active task cannot be safely resumed or replayed.
func prepareTaskStoreForStartup(
	store *localrpc.WorkerTaskStore,
	now time.Time,
) (startupTaskRecovery, error) {
	if store == nil {
		return startupTaskRecovery{}, errors.New("nil Worker task store")
	}
	stats, err := store.Stats()
	if err != nil {
		return startupTaskRecovery{}, err
	}
	output := startupTaskRecovery{}
	cleanupPasses := stats.Tasks/uint64(localrpc.DefaultWorkerMaxPrunePerWrite) + 2
	for pass := uint64(0); pass < cleanupPasses; pass++ {
		removed, hasMore, err := store.Cleanup(
			now, localrpc.DefaultWorkerMaxPrunePerWrite,
		)
		if err != nil {
			return startupTaskRecovery{}, err
		}
		output.ExpiredRemoved += uint64(removed)
		if !hasMore {
			break
		}
		if pass+1 == cleanupPasses {
			return startupTaskRecovery{}, errors.New("Worker task cleanup did not converge")
		}
	}

	stats, err = store.Stats()
	if err != nil {
		return startupTaskRecovery{}, err
	}
	scanPasses := stats.Tasks/uint64(localrpc.DefaultWorkerActiveScanLimit) + 2
	cursor := ""
	for pass := uint64(0); pass < scanPasses; pass++ {
		page, err := store.ScanActiveTasks(
			cursor, localrpc.DefaultWorkerActiveScanLimit, now,
		)
		if err != nil {
			return startupTaskRecovery{}, err
		}
		for _, task := range page.Tasks {
			if _, _, err := store.CompleteTaskFailure(
				task.Identity,
				edgev1.TaskStatus_TASK_STATUS_FAILED,
				now,
				now,
			); err != nil {
				return startupTaskRecovery{}, err
			}
			output.Interrupted++
		}
		if page.NextCursor == "" {
			return output, nil
		}
		if page.NextCursor == cursor {
			return startupTaskRecovery{}, errors.New("Worker active task scan did not advance")
		}
		cursor = page.NextCursor
	}
	return startupTaskRecovery{}, errors.New("Worker active task scan did not converge")
}
