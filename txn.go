package leviq

import (
	"sync"
	"time"

	"github.com/johnsto/leviq/internal"
)

type Batch interface {
	Put(k, v []byte)
	Delete(k []byte)
	Clear()
	Write() error
	Close()
}

// Txn represents a transaction on a queue
type Txn struct {
	queue *Queue
	batch Batch
	puts  *internal.IDHeap // IDs to put
	takes *internal.IDHeap // IDs being taken
	mutex *sync.Mutex
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

	// insert into batch
	if txn.batch == nil {
		txn.batch = txn.queue.Batch()
	}
	dbk := joinKey(txn.queue.ns, k)
	txn.batch.Put(dbk, v)

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

	// Start a new batch
	if txn.batch == nil {
		txn.batch = txn.queue.Batch()
	}

	for i := 0; i < n; i++ {
		txn.takes.Push(ids[i])
		txn.batch.Delete(keys[i])
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

	err := txn.batch.Write()

	if err != nil {
		return err
	}

	txn.queue.putKey(*txn.puts...)
	txn.batch = nil
	txn.puts = internal.NewIDHeap()
	txn.takes = internal.NewIDHeap()

	return nil
}

// Close reverts all changes from the transaction and releases any held
// resources.
func (txn *Txn) Close() error {
	if len(*txn.puts) == 0 && len(*txn.takes) == 0 {
		return nil
	}

	if txn.batch != nil {
		txn.mutex.Lock()
		defer txn.mutex.Unlock()

		// return taken ids to the queue
		txn.queue.putKey(*txn.takes...)

		txn.batch.Clear()
		txn.batch = nil
		txn.puts = internal.NewIDHeap()
		txn.takes = internal.NewIDHeap()
	}
	return nil
}
