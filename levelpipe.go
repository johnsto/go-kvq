package levelpipe

import (
	"log"
	"sync"

	"github.com/jmhodges/levigo"
)

type Pipe struct {
	db    *levigo.DB
	mutex *sync.Mutex
	sync  bool
	ids   *IDHeap
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

// Put starts a new put transaction on the pipe.
func (p *Pipe) Put() *Put {
	return &Put{
		pipe:  p,
		ids:   NewIDHeap(),
		mutex: &sync.Mutex{},
	}
}

// Take starts a new take transaction on the pipe.
func (p *Pipe) Take() *Take {
	return &Take{
		pipe:  p,
		ids:   NewIDHeap(),
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
	}
}

// getKey returns the first available key for taking
func (p *Pipe) getKey() []byte {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	id := p.ids.PopID()
	if id == NilID {
		return nil
	}

	return id.Key()
}

// take takes a single element
func (p *Pipe) take() (id ID, k []byte, v []byte, err error) {
	// get next availble key
	k = p.getKey()
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

// Put encapsulates a put transaction on a pipe.
type Put struct {
	pipe  *Pipe
	batch *levigo.WriteBatch
	ids   *IDHeap
	mutex *sync.Mutex
}

// Put inserts the data into the pipe.
func (put *Put) Put(v []byte) error {
	// get entry ID
	id := NewID()

	// ID => key
	k := id.Key()

	put.mutex.Lock()
	defer put.mutex.Unlock()

	// insert into batch
	if put.batch == nil {
		put.batch = levigo.NewWriteBatch()
	}
	put.batch.Put(k, v)

	// mark as put
	put.ids.Push(id)

	return nil
}

// Close removes all data stored in the Put and prevents further use.
func (put *Put) Close() {
	if put.batch != nil {
		put.mutex.Lock()
		defer put.mutex.Unlock()
		put.batch.Close()
		put.batch = nil
	}
}

// Commit writes the data stored within the Put to disk.
func (put *Put) Commit() error {
	if len(*put.ids) == 0 {
		return nil
	}

	put.mutex.Lock()
	defer put.mutex.Unlock()

	wo := levigo.NewWriteOptions()
	wo.SetSync(put.pipe.sync)
	defer wo.Close()
	err := put.pipe.db.Write(wo, put.batch)

	put.pipe.putKey(*put.ids...)
	put.batch = nil
	put.ids = NewIDHeap()

	return err
}

// Discard removes all data from the Put.
func (put *Put) Discard() error {
	if put.batch != nil {
		put.batch.Clear()
		put.batch = nil
		put.ids = NewIDHeap()
	}
	return nil
}

// Take encapsulates a take transaction on a pipe.
type Take struct {
	pipe  *Pipe
	batch *levigo.WriteBatch
	ids   *IDHeap
	mutex *sync.Mutex
}

// Take gets an item from the pipe.
func (take *Take) Take() ([]byte, error) {
	id, k, v, err := take.pipe.take()
	if err != nil {
		return v, err
	}
	if id == NilID {
		return nil, nil
	}

	take.mutex.Lock()
	defer take.mutex.Unlock()

	if take.batch == nil {
		take.batch = levigo.NewWriteBatch()
	}

	take.ids.Push(id)
	take.batch.Delete(k)

	return v, err
}

// Commit removes the taken items from the pipe.
func (take *Take) Commit() error {
	if len(*take.ids) == 0 {
		return nil
	}

	take.mutex.Lock()
	defer take.mutex.Unlock()

	wo := levigo.NewWriteOptions()
	wo.SetSync(take.pipe.sync)
	defer wo.Close()

	err := take.pipe.db.Write(wo, take.batch)
	if err != nil {
		return err
	}

	take.batch = nil
	take.ids = NewIDHeap()

	return nil
}

// Discard returns the items to the queue.
func (take *Take) Discard() error {
	if len(*take.ids) == 0 {
		return nil
	}

	take.mutex.Lock()
	defer take.mutex.Unlock()

	take.pipe.putKey(*take.ids...)
	take.batch = nil
	take.ids = NewIDHeap()

	return nil
}

// Close closes the transaction.
func (take *Take) Close() error {
	if take.batch != nil {
		take.mutex.Lock()
		defer take.mutex.Unlock()
		take.batch.Clear()
		take.batch = nil
		take.ids = NewIDHeap()
	}
	return nil
}
