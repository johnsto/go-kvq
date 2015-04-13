package leviq

import (
	"errors"
	"sync"
	"time"

	"github.com/johnsto/leviq/backend"
	"github.com/johnsto/leviq/internal"
)

const (
	// DefaultMaxQueue is the default maximum queue capacity.
	DefaultMaxQueue int = 1e6
)

// QueueOptions specifies the operational parameters of a queue
type QueueOptions struct {
	// MaxQueue is the capacity of the queue. Items will start to be rejected
	// if the queue reaches this size.
	MaxQueue int
}

var (
	// DefaultOptions holds the default settings to use when creating a queue.
	DefaultOptions = QueueOptions{
		MaxQueue: DefaultMaxQueue,
	}
	// ErrInsufficientCapacity is returned if the queue does not have enough
	// space to add the requested item(s).
	ErrInsufficientCapacity = errors.New("insufficient queue capacity")
)

type kv struct {
	k []byte
	v []byte
}

// Queue encapsulates a namespaced queue held by a DB.
type Queue struct {
	bucket backend.Bucket
	mutex  *sync.Mutex
	ids    *internal.IDHeap // IDs in queue
	c      chan struct{}    // item availability channel
}

// NewQueue instantiates a new queue from the given database and namespace.
func NewQueue(db backend.DB, namespace string, opts *QueueOptions) (*Queue, error) {
	if opts == nil {
		opts = &DefaultOptions
	}

	bucket, err := db.Bucket(namespace)
	if err != nil {
		return nil, err
	}

	queue := &Queue{
		bucket: bucket,
		mutex:  &sync.Mutex{},
		ids:    internal.NewIDHeap(),
		c:      make(chan struct{}, opts.MaxQueue),
	}
	if err := queue.init(); err != nil {
		return nil, err
	}

	return queue, nil
}

// init populates the queue with all the IDs from the saved database.
func (q *Queue) init() error {
	return q.bucket.ForEach(func(k, v []byte) error {
		// Populate with read keys
		id, err := internal.KeyToID(k)
		if err != nil {
			return err
		}

		q.ids.PushID(id)
		q.c <- struct{}{}
		return nil
	})
}

// Size returns the number of keys currently available within the queue.
// This does not include keys that are in the process of being put or taken.
func (q Queue) Size() int {
	return len(*q.ids)
}

// Clear removes all entries in the DB. Do not call if any transactions are in
// progress.
func (q *Queue) Clear() error {
	return q.bucket.Clear()
}

// Transaction starts a new transaction on the queue.
func (q *Queue) Transaction() *Txn {
	return NewTxn(q)
}

// putKeys adds the ID(s) to the queue, indicating entries that are immediately
// available for taking. Returns number of keys added successfully.
func (q *Queue) putKey(ids ...internal.ID) (int, error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Fail immediately if there isn't enough room in the IDs channel
	if cap(q.c)-len(q.c) < len(ids) {
		return 0, ErrInsufficientCapacity
	}

	// Add each item to the availability channel
	n := 0
	for _, id := range ids {
		select {
		case q.c <- struct{}{}:
			q.ids.PushID(id)
			n++
		default:
			// This case shouldn't happen in practise, but better to catch
			// than block.
			return n, ErrInsufficientCapacity
		}
	}
	return n, nil
}

// getKeys returns upto `n` keys available for immediate taking, removing them
// from the set of keys and returns them to the caller.
func (q *Queue) getKeys(n int) [][]byte {
	b := [][]byte{}
	for {
		select {
		case <-q.c:
			// Key became available, add to list of returned values
			q.mutex.Lock()
			k := q.ids.PopID().Key()
			q.mutex.Unlock()
			b = append(b, k)
			// Have we got enough values now?
			if len(b) == n {
				return b
			}
		default:
			// Ran out of keys
			return b
		}
	}
}

// awaitKeys returns `n` keys available for taking, removing them from the set
// of keys and returns them to the caller, waiting at most the specified amount
// of time forkeys to become available before before returning nil.
func (q *Queue) awaitKeys(n int, t time.Duration) [][]byte {
	if t == 0 {
		// Special case - get keys directly without timeout if duration is zero
		return q.getKeys(n)
	}

	cancel := make(chan struct{}, 0)
	timeout := time.AfterFunc(t, func() {
		close(cancel)
	})
	defer timeout.Stop()

	// Listen for available keys
	b := [][]byte{}
	for {
		select {
		case <-q.c:
			// Key became available, add to list of returned values
			q.mutex.Lock()
			k := q.ids.PopID().Key()
			q.mutex.Unlock()
			b = append(b, k)
			// Have we got enough values now?
			if len(b) == n {
				return b
			}
		case <-cancel:
			// Timed out; return whatever values we got in that time
			return b
		}
	}
}

// take takes `n` elements from the queue, waiting at most `t` to retrieve them.
func (q *Queue) take(n int, t time.Duration) (ids []internal.ID, keys [][]byte, values [][]byte, err error) {
	// Fetch available keys
	keys = q.awaitKeys(n, t)

	// Setup return structures
	n = len(keys)
	ids = make([]internal.ID, n)
	values = make([][]byte, n)

	// Populate return structures
	for i, k := range keys {
		values[i], err = q.bucket.Get(k)
		if err != nil {
			return nil, nil, nil, err
		}
		ids[i], err = internal.KeyToID(k)
	}

	return ids, keys, values, err
}

// enact puts and takes the given key values to the underlying storage.
func (q *Queue) enact(puts, takes []kv) error {
	return q.bucket.Batch(func(b backend.Batch) error {
		for _, kv := range puts {
			b.Put(kv.k, kv.v)
		}
		for _, kv := range takes {
			b.Delete(kv.k)
		}
		return nil
	})
}
