package main

import (
	"encoding/binary"
	"log"
	"sort"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/sdming/gosnow"
)

var snow *gosnow.SnowFlake

type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Uint64Slice) Sort()              { sort.Sort(p) }
func (p Uint64Slice) Search(v uint64) int {
	return sort.Search(len(p), func(i int) bool {
		return p[i] >= v
	})
}

func init() {
	var err error
	snow, err = gosnow.Default()
	if err != nil {
		log.Fatalln(err)
	}
}

type Pipe struct {
	db    *levigo.DB
	mutex *sync.Mutex
	sync  bool
	// ids is the set of all keys in the pipe
	ids []uint64
	// taken is the set of keys currently in a taken transaction
	taken map[uint64]struct{}
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
		ids:   make([]uint64, 0),
		taken: make(map[uint64]struct{}),
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
		id, n := binary.Uvarint(it.Key())
		if n <= 0 {
			log.Fatalln("couldn't parse key to ID", it.Key())
		}
		p.ids = append(p.ids, id)
	}
}

func (p *Pipe) SetSync(sync bool) {
	p.sync = sync
}

func (p *Pipe) findFirstKey() uint64 {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	it := p.db.NewIterator(ro)
	defer it.Close()
	it.SeekToFirst()
	if it.Valid() {
		id, n := binary.Uvarint(it.Key())
		if n <= 0 {
			log.Fatalln("couldn't parse key to ID", it.Key())
		}
		return id
	}
	return 0
}

func (p *Pipe) Close() {
	p.db.Close()
}

func (p *Pipe) Put() *Put {
	return &Put{
		pipe:  p,
		puts:  make(map[uint64]struct{}),
		mutex: &sync.Mutex{},
	}
}

// add remembers the given entry IDs in the pipe
func (p *Pipe) add(ids ...uint64) {
	p.ids = append(p.ids, ids...)
	Uint64Slice(p.ids).Sort()
}

// remove forgets the given entry IDs from the pipe
func (p *Pipe) remove(ids ...uint64) {
	for _, id := range ids {
		pos := Uint64Slice(p.ids).Search(id)
		if len(p.ids) > pos {
			p.ids = append(p.ids[:pos], p.ids[pos+1:]...)
		} else {
			p.ids = p.ids[:pos]
		}
	}
}

func (p *Pipe) Take() *Take {
	return &Take{
		pipe:  p,
		taken: make(map[uint64]struct{}),
		mutex: &sync.Mutex{},
	}
}

// take takes a single element
func (p *Pipe) take() (id uint64, k []byte, v []byte, err error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// find first likely available key
	k = p.firstKey()
	if k == nil {
		return 0, nil, nil, nil
	}

	// retrieve value
	ro := levigo.NewReadOptions()
	v, err = p.db.Get(ro, k)
	if err != nil {
		return 0, nil, nil, err
	}

	// key => id
	id, n := binary.Uvarint(k)
	if n <= 0 {
		log.Fatalln("couldn't parse key: " + string(k))
	}

	// mark as taken
	p.mark(id)

	return id, k, v, nil
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
	defer wo.Close()
	wo.SetSync(p.sync)
	return p.db.Write(wo, b)
}

// firstKey returns the first available key for taking
func (p *Pipe) firstKey() []byte {
	for _, id := range p.ids {
		if _, ok := p.taken[id]; !ok {
			k := make([]byte, 16)
			if binary.PutUvarint(k, id) <= 0 {
				log.Fatalln("couldn't write key")
			}
			return k
		}
	}
	return nil
}

// mark sets the 'taken' flag for an entry ID, preventing further takes.
func (p *Pipe) mark(k uint64) {
	p.taken[k] = struct{}{}
}

// unmark clears the 'taken' flag for an entry ID, making it available to be
// taken.
func (p *Pipe) unmark(id uint64) {
	delete(p.taken, id)
}

type Put struct {
	pipe  *Pipe
	batch *levigo.WriteBatch
	puts  map[uint64]struct{}
	mutex *sync.Mutex
}

// Put inserts the data into the pipe.
func (put *Put) Put(v []byte) error {
	// get entry ID
	id, err := snow.Next()
	if err != nil {
		return err
	}

	// ID => key
	k := make([]byte, 16)
	if binary.PutUvarint(k, id) <= 0 {
		log.Fatalln("couldn't write key")
	}

	put.mutex.Lock()
	defer put.mutex.Unlock()

	// insert into batch
	if put.batch == nil {
		put.batch = levigo.NewWriteBatch()
	}
	put.batch.Put(k, v)

	// mark as put
	put.pipe.mutex.Lock()
	defer put.pipe.mutex.Unlock()
	put.puts[id] = struct{}{}

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
	put.mutex.Lock()
	defer put.mutex.Unlock()
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(put.pipe.sync)
	err := put.pipe.db.Write(wo, put.batch)
	for id := range put.puts {
		put.pipe.add(id)
	}
	put.batch = nil
	return err
}

// Discard removes all data from the Put.
func (put *Put) Discard() error {
	if put.batch != nil {
		put.batch.Clear()
		put.batch = nil
	}
	return nil
}

type Take struct {
	pipe  *Pipe
	batch *levigo.WriteBatch
	taken map[uint64]struct{}
	mutex *sync.Mutex
}

// Take gets an item from the pipe.
func (take *Take) Take() ([]byte, error) {
	id, k, v, err := take.pipe.take()
	if err != nil {
		return v, err
	}
	if id == 0 {
		return nil, nil
	}
	take.mutex.Lock()
	defer take.mutex.Unlock()
	if take.batch == nil {
		take.batch = levigo.NewWriteBatch()
	}
	take.taken[id] = struct{}{}
	take.batch.Delete(k)
	return v, err
}

// Commit removes the taken items from the pipe.
func (take *Take) Commit() error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(take.pipe.sync)
	take.mutex.Lock()
	defer take.mutex.Unlock()
	err := take.pipe.db.Write(wo, take.batch)
	if err != nil {
		return err
	}
	for k := range take.taken {
		take.pipe.unmark(k)
		take.pipe.remove(k)
	}
	take.taken = make(map[uint64]struct{})
	return nil
}

// Discard returns the items to the queue.
func (take *Take) Discard() error {
	take.mutex.Lock()
	defer take.mutex.Unlock()
	for k := range take.taken {
		take.pipe.unmark(k)
	}
	take.batch = nil
	return nil
}

// Close closes the transaction.
func (take *Take) Close() error {
	if take.batch != nil {
		take.mutex.Lock()
		defer take.mutex.Unlock()
		take.batch.Clear()
		take.batch = nil
	}
	return nil
}

type Putter interface {
	Put(v []byte) error
	Commit() error
	Discard() error
	Close() error
}

type Taker interface {
	Take() ([]byte, error)
	Commit() error
	Discard() error
	Close() error
}
