package levelpipe

import (
	"log"
	"sync"
	"time"

	"github.com/jmhodges/levigo"
)

type Pipe struct {
	db    *levigo.DB
	mutex *sync.Mutex
	ids   *IDHeap // IDs in pipe
	sync  bool    // true if transactions should be synced
	c     chan struct{}
}

type Txn struct {
	pipe  *Pipe
	batch *levigo.WriteBatch
	puts  *IDHeap // IDs to put
	takes *IDHeap // IDs being taken
	mutex *sync.Mutex
}

// DestroyPipe destroys the pipe at the given path.
func DestroyPipe(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

// NewPipe creates a new pipe at the given path.
func NewPipe(path string) (*Pipe, error) {
	return open(path, true)
}

// OpenPipe opens the pipe at the given path.
func OpenPipe(path string) (*Pipe, error) {
	return open(path, false)
}

func open(path string, create bool) (*Pipe, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(create)
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	pipe := &Pipe{
		db:    db,
		mutex: &sync.Mutex{},
		ids:   NewIDHeap(),
		c:     make(chan struct{}, 1e6),
	}
	pipe.init()
	return pipe, nil
}

// init populates the pipe with all the IDs in the database.
func (p *Pipe) init() {
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
func (p *Pipe) SetSync(sync bool) {
	p.sync = sync
}

// Close closes the pipe.
func (p *Pipe) Close() {
	p.db.Close()
}

// Transaction starts a new transaction.
func (p *Pipe) Transaction() *Txn {
	return &Txn{
		pipe:  p,
		puts:  NewIDHeap(),
		takes: NewIDHeap(),
		mutex: &sync.Mutex{},
	}
}

// putKeys adds the ID(s) to the pipe, indicating entries that are immediately
// available for taking.
func (p *Pipe) putKey(ids ...ID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, id := range ids {
		p.ids.PushID(id)
		p.c <- struct{}{}
	}
}

// getKey returns the first available key for taking
func (p *Pipe) getKey(t time.Duration) []byte {
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

// awaitKey returns the first available key for taking within the given time
// window
func (p *Pipe) awaitKey(t time.Duration) []byte {
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

// take takes a single element
func (p *Pipe) take(t time.Duration) (id ID, k []byte, v []byte, err error) {
	// get next availble key
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

// Clear removes all entries in the db
func (p *Pipe) Clear() error {
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

// Put inserts the data into the pipe.
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

// Take gets an item from the pipe.
func (txn *Txn) Take() ([]byte, error) {
	return txn.TakeWait(0)
}

// Take gets an item from the pipe, waiting at most `t` before returning nil.
func (txn *Txn) TakeWait(t time.Duration) ([]byte, error) {
	id, k, v, err := txn.pipe.take(t)
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
	wo.SetSync(txn.pipe.sync)
	defer wo.Close()
	err := txn.pipe.db.Write(wo, txn.batch)

	txn.pipe.putKey(*txn.puts...)
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

		// return taken ids to the pipe
		txn.pipe.putKey(*txn.takes...)

		txn.batch.Clear()
		txn.batch = nil
		txn.puts = NewIDHeap()
		txn.takes = NewIDHeap()
	}
	return nil
}
