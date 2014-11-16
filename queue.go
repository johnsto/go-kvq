package leviq

import (
	"log"
	"sync"
	"time"

	"github.com/jmhodges/levigo"
)

type Queue struct {
	db    *levigo.DB
	mutex *sync.Mutex
	ids   *IDHeap // IDs in queue
	sync  bool    // true if transactions should be synced
	c     chan struct{}
}

type Txn struct {
	queue *Queue
	batch *levigo.WriteBatch
	puts  *IDHeap // IDs to put
	takes *IDHeap // IDs being taken
	mutex *sync.Mutex
}

// DestroyQueue destroys the queue at the given path.
func DestroyQueue(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

// NewQueue creates a new queue at the given path.
func NewQueue(path string) (*Queue, error) {
	return open(path, true)
}

// OpenQueue opens the queue at the given path.
func OpenQueue(path string) (*Queue, error) {
	return open(path, false)
}

func open(path string, create bool) (*Queue, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(create)
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	queue := &Queue{
		db:    db,
		mutex: &sync.Mutex{},
		ids:   NewIDHeap(),
		c:     make(chan struct{}, 1e6),
	}
	queue.init()
	return queue, nil
}

// init populates the queue with all the IDs from the saved database.
func (p *Queue) init() {
	ro := levigo.NewReadOptions()
	defer ro.Close()

	it := p.db.NewIterator(ro)
	defer it.Close()

	it.SeekToFirst()
	if it.Valid() {
		id, err := KeyToID(it.Key())
		if err != nil {
			log.Fatalln(err)
		}
		p.ids.PushID(id)
	}
}

// SetSync specifies if the LevelDB database should be sync'd to disk before
// returning from any commit operations. Set this to true for increased
// data durability at the cost of transaction commit time.
func (p *Queue) SetSync(sync bool) {
	p.sync = sync
}

// Clear removes all entries in the DB. Do not call if any transactions are in
// progress.
func (p *Queue) Clear() error {
	ro := levigo.NewReadOptions()
	defer ro.Close()

	b := levigo.NewWriteBatch()
	it := p.db.NewIterator(ro)
	it.SeekToFirst()
	for it.Valid() {
		b.Delete(it.Key())
	}

	wo := levigo.NewWriteOptions()
	wo.SetSync(p.sync)
	defer wo.Close()

	return p.db.Write(wo, b)
}

// Close closes the queue.
func (p *Queue) Close() {
	p.db.Close()
}

// Transaction starts a new transaction.
func (p *Queue) Transaction() *Txn {
	return &Txn{
		queue: p,
		puts:  NewIDHeap(),
		takes: NewIDHeap(),
		mutex: &sync.Mutex{},
	}
}

// putKeys adds the ID(s) to the queue, indicating entries that are immediately
// available for taking.
func (p *Queue) putKey(ids ...ID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, id := range ids {
		p.ids.PushID(id)
		p.c <- struct{}{}
	}
}

// getKey finds the first key available for taking, removes it from the set of
// keys and returns it to the caller. If the duration argument is greater than
// 0, it will wait the prescribed time for a key to arrive before returning nil.
func (p *Queue) getKey(t time.Duration) []byte {
	select {
	case <-p.c:
		p.mutex.Lock()
		defer p.mutex.Unlock()
		return p.ids.PopID().Key()
	default:
		if t == 0 {
			return nil
		} else {
			return p.awaitKey(t)
		}
	}
}

// awaitKey finds the first key available for taking, removes it from the set
// of keys and returns it to the caller, waiting at most the specified amount
// of time for a key to become available before before returning nil.
func (p *Queue) awaitKey(t time.Duration) []byte {
	cancel := make(chan struct{}, 0)
	timeout := time.AfterFunc(t, func() {
		close(cancel)
	})
	defer timeout.Stop()

	select {
	case <-p.c:
		p.mutex.Lock()
		defer p.mutex.Unlock()
		return p.ids.PopID().Key()
	case <-cancel:
		return nil
	}
}

// take takes a single element.
func (p *Queue) take(t time.Duration) (id ID, k []byte, v []byte, err error) {
	// get next available key
	k = p.getKey(t)
	if k == nil {
		return NilID, nil, nil, nil
	}

	// retrieve value
	ro := levigo.NewReadOptions()
	v, err = p.db.Get(ro, k)
	if err != nil {
		return NilID, nil, nil, err
	}

	// key => id
	id, err = KeyToID(k)

	return id, k, v, err
}

// Put inserts the data into the queue.
func (txn *Txn) Put(v []byte) error {
	if v == nil {
		return nil
	}

	// get entry ID
	id := NewID()

	// ID => key
	k := id.Key()

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	// insert into batch
	if txn.batch == nil {
		txn.batch = levigo.NewWriteBatch()
	}
	txn.batch.Put(k, v)

	// mark as put
	txn.puts.Push(id)

	return nil
}

// Take gets an item from the queue.
func (txn *Txn) Take() ([]byte, error) {
	return txn.TakeWait(0)
}

// Take gets an item from the queue, waiting at most `t` before returning nil.
func (txn *Txn) TakeWait(t time.Duration) ([]byte, error) {
	id, k, v, err := txn.queue.take(t)
	if err != nil {
		return v, err
	}
	if id == NilID {
		return nil, nil
	}

	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	if txn.batch == nil {
		txn.batch = levigo.NewWriteBatch()
	}

	txn.takes.Push(id)
	txn.batch.Delete(k)

	return v, err
}

// Commit writes the transaction to disk.
func (txn *Txn) Commit() error {
	txn.mutex.Lock()
	defer txn.mutex.Unlock()

	if len(*txn.puts) == 0 && len(*txn.takes) == 0 {
		return nil
	}

	wo := levigo.NewWriteOptions()
	wo.SetSync(txn.queue.sync)
	defer wo.Close()
	err := txn.queue.db.Write(wo, txn.batch)

	txn.queue.putKey(*txn.puts...)
	txn.batch = nil
	txn.puts = NewIDHeap()
	txn.takes = NewIDHeap()

	return err
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
		txn.puts = NewIDHeap()
		txn.takes = NewIDHeap()
	}
	return nil
}
