package kvq

import (
	"sync"
	"time"

	"github.com/johnsto/go-kvq/kvq/internal"
)

// Txn represents a transaction on a Queue
type Txn struct {
	queue      *Queue
	puts       *internal.IDHeap // IDs to put
	takes      *internal.IDHeap // IDs being taken
	putValues  []kv
	takeValues []kv
	mutex      *sync.Mutex
}

// NewTxn returns a new Txn that operates on the given Queue.
func NewTxn(q *Queue) *Txn {
	txn := &Txn{
		queue: q,
		mutex: &sync.Mutex{},
	}
	txn.Reset()
	return txn
}

// Reset empties the transaction and resets it to an empty (default) state.
func (txn *Txn) Reset() {
	txn.puts = internal.NewIDHeap()
	txn.takes = internal.NewIDHeap()
	txn.putValues = make([]kv, 0)
	txn.takeValues = make([]kv, 0)
}

// Put inserts the data into the queue.
func (txn *Txn) Put(v []byte) error {
	if v == nil {
		return nil
	}

	// get entry ID and key
	id := internal.NewID()
	k := id.Key()

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Add put value onto put queue
	txn.putValues = append(txn.putValues, kv{k, v})

	// Mark this ID as being put
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
	// Retrieve available values from storage
	ids, keys, values, err := txn.queue.take(n, t)
	if err != nil {
		return nil, err
	}

	// No items available? Return without failure
	if len(ids) == 0 {
		return nil, nil
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Push taken items onto reserved queue
	n = len(ids)
	for i := 0; i < n; i++ {
		txn.takes.Push(ids[i])
		txn.takeValues = append(txn.takeValues, kv{keys[i], values[i]})
	}

	return values, err
}

// Commit writes transaction to storage. The Txn will remain valid for further
// use.
func (txn *Txn) Commit() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Do nothing if there's nothing to do
	if len(*txn.puts) == 0 && len(*txn.takes) == 0 {
		return nil
	}

	// Put/take keys from backend storage
	if err := txn.queue.enact(txn.putValues, txn.takeValues); err != nil {
		return err
	}

	// Add keys to availability queue
	_, err := txn.queue.putKey(*txn.puts...)
	if err != nil {
		return err
	}

	txn.Reset()
	return nil
}

// Close reverts all changes from the transaction and releases any held
// resources. The Txn will remain valid for further use.
func (txn *Txn) Close() error {
	if len(*txn.puts) == 0 && len(*txn.takes) == 0 {
		return nil
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// Return taken ids to the queue
	txn.queue.putKey(*txn.takes...)

	txn.Reset()
	return nil
}
