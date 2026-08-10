package thirdparty

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

var completionsBucket = []byte("thirdparty-completions-v1")

// pruneEvery bounds how often stabilize's write path also sweeps expired
// records -- pruning on every call would make each Invoke/Query pay an
// O(bucket size) scan; pruning this rarely still keeps the store bounded
// for a long-running worker without materially delaying any one request.
const pruneEvery = 128

type completionRecord struct {
	CompletedUnixMillis   int64 `json:"completed_unix_millis"`
	RetainUntilUnixMillis int64 `json:"retain_until_unix_millis"`
}

// completionStore durably records the first-observed completion time for
// a terminal third-party invocation, keyed by request_id -- the worker-
// restart-surviving counterpart to the in-memory cache this replaced. Each
// record is bounded by its own retain_until_unix_millis (mirroring
// ThirdPartyInvokeRequest.retain_until_unix_millis, itself mirroring the
// native path's InvokeRequest.retain_until_unix_millis), so the store
// cannot grow without bound for the lifetime of a long-running worker
// process the way an un-evicted in-memory map would.
type completionStore struct {
	db      *bolt.DB
	writes  atomic.Uint64
	nowFunc func() time.Time
}

func openCompletionStore(path string) (*completionStore, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("thirdparty: open completion store: %w", err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(completionsBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("thirdparty: init completion store: %w", err)
	}
	return &completionStore{db: db, nowFunc: time.Now}, nil
}

func (c *completionStore) close() error {
	return c.db.Close()
}

// stabilize returns the first-ever observed completion time for
// requestID, durably recording (observed, retainUntil) if no record
// exists yet. retainUntil <= 0 is treated as "already expired, do not
// retain" rather than "retain forever" -- callers MUST supply a real,
// bounded retention boundary from the originating request, exactly the
// same trust assumption the native model-serving path's WorkerTaskStore
// already places on retain_until_unix_millis.
func (c *completionStore) stabilize(requestID string, observed, retainUntil int64) (int64, error) {
	var result int64
	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(completionsBucket)
		if existing := b.Get([]byte(requestID)); existing != nil {
			var rec completionRecord
			if err := json.Unmarshal(existing, &rec); err != nil {
				return fmt.Errorf("thirdparty: corrupt completion record for %q: %w", requestID, err)
			}
			result = rec.CompletedUnixMillis
			return nil
		}
		rec := completionRecord{CompletedUnixMillis: observed, RetainUntilUnixMillis: retainUntil}
		encoded, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		result = observed
		return b.Put([]byte(requestID), encoded)
	})
	if err != nil {
		return 0, err
	}
	if c.writes.Add(1)%pruneEvery == 0 {
		_ = c.prune(c.nowFunc())
	}
	return result, nil
}

// prune deletes every record whose retain_until_unix_millis has passed.
// Called opportunistically from stabilize (see pruneEvery) rather than on
// a separate timer -- this store has no background goroutine of its own,
// matching every other piece of per-request state in this package.
func (c *completionStore) prune(now time.Time) error {
	nowMillis := now.UnixMilli()
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(completionsBucket)
		var expired [][]byte
		if err := b.ForEach(func(k, v []byte) error {
			var rec completionRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				// A corrupt record can never be recovered by retrying --
				// drop it rather than letting one bad entry block pruning
				// of every legitimate expired record forever.
				expired = append(expired, append([]byte(nil), k...))
				return nil
			}
			if rec.RetainUntilUnixMillis <= nowMillis {
				expired = append(expired, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range expired {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}
