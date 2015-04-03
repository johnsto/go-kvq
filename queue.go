package leviq

import (
	"sync"
	"time"

	"github.com/johnsto/leviq/backend"
	"github.com/johnsto/leviq/internal"
)

const (
	MaxQueue int = 1e6
)

// Queue encapsulates a namespaced queue held by a DB.
type Queue struct {
	backend.Queue
	mutex *sync.Mutex
	ids   *internal.IDHeap // IDs in queue
	c     chan struct{}    // item availability channel
}

// init populates the queue with all the IDs from the saved database.
func (q *Queue) init() error {
	return q.ForEach(func(k, v []byte) error {
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
	return q.Queue.Clear()
}

// Transaction starts a new transaction on the queue.
func (q *Queue) Transaction() *Txn {
	return &Txn{
		queue:      q,
		puts:       internal.NewIDHeap(),
		takes:      internal.NewIDHeap(),
		putValues:  make([]kv, 0),
		takeValues: make([][]byte, 0),
		mutex:      &sync.Mutex{},
	}
}

// putKeys adds the ID(s) to the queue, indicating entries that are immediately
// available for taking.
func (q *Queue) putKey(ids ...internal.ID) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	for _, id := range ids {
		q.ids.PushID(id)
		q.c <- struct{}{}
	}
}

// getKeys returns upto `n` keys available for immediate taking, removing them
// from the set of keys and returns them to the caller.
func (q *Queue) getKeys(n int) [][]byte {
	b := [][]byte{}
	for {
		select {
		case <-q.c:
			q.mutex.Lock()
			k := q.ids.PopID().Key()
			q.mutex.Unlock()
			b = append(b, k)
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
		return q.getKeys(n)
	}

	cancel := make(chan struct{}, 0)
	timeout := time.AfterFunc(t, func() {
		close(cancel)
	})
	defer timeout.Stop()

	b := [][]byte{}
	for {
		select {
		case <-q.c:
			q.mutex.Lock()
			k := q.ids.PopID().Key()
			q.mutex.Unlock()
			b = append(b, k)
			if len(b) == n {
				return b
			}
		case <-cancel:
			// Timed out
			return b
		}
	}
}

// take takes `n` elements from the queue, waiting at most `t` to retrieve them.
func (q *Queue) take(n int, t time.Duration) (ids []internal.ID, keys [][]byte, values [][]byte, err error) {
	// get next available key
	keys = q.awaitKeys(n, t)

	n = len(keys)
	ids = make([]internal.ID, n)
	values = make([][]byte, n)

	for i, k := range keys {
		// retrieve value
		values[i], err = q.Queue.Get(k)
		if err != nil {
			return nil, nil, nil, err
		}

		// key => id
		ids[i], err = internal.KeyToID(k)
	}

	return ids, keys, values, err
}
