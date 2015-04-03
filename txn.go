package leviq

import (
	"sync"
	"time"

	"github.com/johnsto/leviq/backend"
	"github.com/johnsto/leviq/internal"
)

type kv struct {
	k []byte
	v []byte
}

// Txn represents a transaction on a queue
type Txn struct {
	queue      *Queue
	puts       *internal.IDHeap // IDs to put
	takes      *internal.IDHeap // IDs being taken
	putValues  []kv
	takeValues [][]byte
	mutex      *sync.Mutex
}

// Put inserts the data into the queue.
func (txn *Txn) Put(v []byte) error {
	if v == nil {
		return nil
	}

	// get entry ID
	id := internal.NewID()

	// ID => key
	k := id.Key()

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	txn.putValues = append(txn.putValues, kv{k, v})

	// mark as put
	txn.puts.Push(id)

	return nil
}

// Take gets an item from the queue, returning nil if no items are available.
func (txn *Txn) Take() ([]byte, error) {
	b, err := txn.TakeN(1, 0)
	if b == nil {
		return nil, err
	}
	return b[0], nil
}

// TakeN gets `n` items from the queue, waiting at most `t` for them to all
// become available. If no items are available, nil is returned.
func (txn *Txn) TakeN(n int, t time.Duration) ([][]byte, error) {
	ids, keys, values, err := txn.queue.take(n, t)
	if err != nil {
		return nil, err
	}

	if len(ids) == 0 {
		return nil, nil
	}
	n = len(ids)

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	for i := 0; i < n; i++ {
		txn.takes.Push(ids[i])
		txn.takeValues = append(txn.takeValues, keys[i])
	}

	return values, err
}

// Commit writes the transaction to disk.
func (txn *Txn) Commit() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	if len(*txn.puts) == 0 && len(*txn.takes) == 0 {
		return nil
	}

	err := txn.queue.Batch(func(b backend.Batch) error {
		for _, kv := range txn.putValues {
			b.Put(kv.k, kv.v)
		}
		for _, k := range txn.takeValues {
			b.Delete(k)
		}
		return nil
	})

	if err != nil {
		return err
	}

	txn.queue.putKey(*txn.puts...)
	txn.puts = internal.NewIDHeap()
	txn.takes = internal.NewIDHeap()
	txn.putValues = make([]kv, 0)
	txn.takeValues = make([][]byte, 0)
	return nil
}

// Close reverts all changes from the transaction and releases any held
// resources.
func (txn *Txn) Close() error {
	if len(*txn.puts) == 0 && len(*txn.takes) == 0 {
		return nil
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// return taken ids to the queue
	txn.queue.putKey(*txn.takes...)

	txn.puts = internal.NewIDHeap()
	txn.takes = internal.NewIDHeap()
	txn.putValues = make([]kv, 0)
	txn.takeValues = make([][]byte, 0)

	return nil
}
