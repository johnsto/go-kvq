package leviq

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/jmhodges/levigo"
	"github.com/johnsto/leviq/internal"
)

type DB struct {
	db *levigo.DB
}

type Queue struct {
	ns    []byte
	db    *DB
	mutex *sync.Mutex
	ids   *internal.IDHeap // IDs in queue
	sync  bool             // true if transactions should be synced
	c     chan struct{}
}

type Txn struct {
	queue *Queue
	batch *levigo.WriteBatch
	puts  *internal.IDHeap // IDs to put
	takes *internal.IDHeap // IDs being taken
	mutex *sync.Mutex
}

// Destroy destroys the queue at the given path.
func Destroy(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

// OpenQueue opens the queue at the given path.
func Open(path string, opts *levigo.Options) (*DB, error) {
	if opts == nil {
		opts = levigo.NewOptions()
		opts.SetCreateIfMissing(true)
	}

	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &DB{
		db: db,
	}, nil
}

func (db *DB) Queue(ns string) *Queue {
	queue := &Queue{
		ns:    []byte(ns),
		db:    db,
		mutex: &sync.Mutex{},
		ids:   internal.NewIDHeap(),
		c:     make(chan struct{}, 1e6),
	}
	queue.init()
	return queue
}

// Close closes the queue.
func (db *DB) Close() {
	db.db.Close()
}

func splitKey(ns, key []byte) (k []byte) {
	prefix := append(ns, 0)
	if !bytes.HasPrefix(key, prefix) {
		return nil
	}
	return key[len(ns)+1:]
}

func joinKey(ns, k []byte) (key []byte) {
	return bytes.Join([][]byte{ns, k}, []byte{0})
}

// init populates the queue with all the IDs from the saved database.
func (q *Queue) init() {
	ro := levigo.NewReadOptions()
	defer ro.Close()

	it := q.db.db.NewIterator(ro)
	defer it.Close()

	it.Seek(q.ns)
	for it.Valid() {
		k := splitKey(q.ns, it.Key())
		if k == nil {
			break
		}
		id, err := internal.KeyToID(k)
		if err != nil {
			log.Fatalln(err)
		}
		q.ids.PushID(id)
		p.c <- struct{}{}
		it.Next()
	}
}

// SetSync specifies if the LevelDB database should be sync'd to disk before
// returning from any commit operations. Set this to true for increased
// data durability at the cost of transaction commit time.
func (q *Queue) SetSync(sync bool) {
	q.sync = sync
}

// Clear removes all entries in the DB. Do not call if any transactions are in
// progress.
func (q *Queue) Clear() error {
	ro := levigo.NewReadOptions()
	defer ro.Close()

	b := levigo.NewWriteBatch()
	it := q.db.db.NewIterator(ro)
	it.Seek(q.ns)
	for it.Valid() {
		k := splitKey(q.ns, it.Key())
		if k == nil {
			break
		}
		b.Delete(it.Key())
	}

	wo := levigo.NewWriteOptions()
	wo.SetSync(q.sync)
	defer wo.Close()

	return q.db.db.Write(wo, b)
}

// Transaction starts a new transaction.
func (q *Queue) Transaction() *Txn {
	return &Txn{
		queue: q,
		puts:  internal.NewIDHeap(),
		takes: internal.NewIDHeap(),
		mutex: &sync.Mutex{},
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

// getKey finds the first key available for taking, removes it from the set of
// keys and returns it to the caller. If the duration argument is greater than
// 0, it will wait the prescribed time for a key to arrive before returning nil.
func (q *Queue) getKey(t time.Duration) []byte {
	select {
	case <-q.c:
		q.mutex.Lock()
		defer q.mutex.Unlock()
		return q.ids.PopID().Key()
	default:
		if t == 0 {
			return nil
		} else {
			return q.awaitKey(t)
		}
	}
}

// awaitKey finds the first key available for taking, removes it from the set
// of keys and returns it to the caller, waiting at most the specified amount
// of time for a key to become available before before returning nil.
func (q *Queue) awaitKey(t time.Duration) []byte {
	cancel := make(chan struct{}, 0)
	timeout := time.AfterFunc(t, func() {
		close(cancel)
	})
	defer timeout.Stop()

	select {
	case <-q.c:
		q.mutex.Lock()
		defer q.mutex.Unlock()
		return q.ids.PopID().Key()
	case <-cancel:
		return nil
	}
}

// take takes a single element.
func (q *Queue) take(t time.Duration) (id internal.ID, k []byte, v []byte, err error) {
	// get next available key
	k = q.getKey(t)
	if k == nil {
		return internal.NilID, nil, nil, nil
	}

	// retrieve value
	ro := levigo.NewReadOptions()
	dbk := joinKey(q.ns, k)
	v, err = q.db.db.Get(ro, dbk)
	if err != nil {
		return internal.NilID, nil, nil, err
	}

	// key => id
	id, err = internal.KeyToID(k)

	return id, k, v, err
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
		txn.batch = levigo.NewWriteBatch()
	}
	dbk := joinKey(txn.queue.ns, k)
	txn.batch.Put(dbk, v)

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
	if id == internal.NilID {
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
	err := txn.queue.db.db.Write(wo, txn.batch)

	txn.queue.putKey(*txn.puts...)
	txn.batch = nil
	txn.puts = internal.NewIDHeap()
	txn.takes = internal.NewIDHeap()

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
		txn.puts = internal.NewIDHeap()
		txn.takes = internal.NewIDHeap()
	}
	return nil
}
