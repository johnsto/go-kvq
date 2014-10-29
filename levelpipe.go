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
		return p[i] == v
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
	taken map[uint64]struct{}
	mutex *sync.Mutex
	keys  []uint64
	first uint64
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
		taken: make(map[uint64]struct{}),
		mutex: &sync.Mutex{},
		keys:  make([]uint64, 0),
	}
	pipe.init()
	return pipe, nil
}

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
		p.keys = append(p.keys, id)
	}
	Uint64Slice(p.keys).Sort()
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

func (p *Pipe) Put() Put {
	return Put{
		p:     p,
		b:     levigo.NewWriteBatch(),
		mutex: &sync.Mutex{},
		puts:  make(map[uint64]struct{}),
		first: 0,
	}
}

func (p *Pipe) put(ids ...uint64) {
	p.keys = append(p.keys, ids...)
	Uint64Slice(p.keys).Sort()
}

func (p *Pipe) take(ids ...uint64) {
	for _, id := range ids {
		pos := Uint64Slice(p.keys).Search(id)
		if len(p.keys) > pos {
			p.keys = append(p.keys[:pos], p.keys[pos+1:]...)
		} else {
			p.keys = p.keys[:pos]
		}
	}
}

func (p *Pipe) Take() Take {
	ro := levigo.NewReadOptions()
	return Take{
		p:     p,
		b:     levigo.NewWriteBatch(),
		taken: make(map[uint64]struct{}),
		mutex: &sync.Mutex{},
		it:    p.db.NewIterator(ro),
	}
}

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
	wo.SetSync(true)
	return p.db.Write(wo, b)
}

// firstKey returns the first available key for taking
func (p *Pipe) firstKey() []byte {
	for _, id := range p.keys {
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
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.taken[k] = struct{}{}
}

// unmark clears the 'taken' flag for an entry ID, making it available to be
// taken.
func (p *Pipe) unmark(id uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.taken, id)
}

type Put struct {
	p     *Pipe
	b     *levigo.WriteBatch
	puts  map[uint64]struct{}
	first uint64
	mutex *sync.Mutex
}

// Put inserts the data into the pipe.
func (p *Put) Put(v []byte) error {
	id, err := snow.Next()
	if err != nil {
		return err
	}
	k := make([]byte, 16)
	if binary.PutUvarint(k, id) <= 0 {
		log.Fatalln("couldn't write key")
	}
	p.b.Put(k, v)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.puts[id] = struct{}{}
	if p.first == 0 || id < p.first {
		p.first = id
	}
	return nil
}

// Close removes all data stored in the Put and prevents further use.
func (p *Put) Close() {
	p.b.Close()
}

// Commit writes the data stored within the Put to disk.
func (p *Put) Commit() error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(true)
	err := p.p.db.Write(wo, p.b)
	for id := range p.puts {
		p.p.put(id)
	}
	return err
}

// Discard removes all data from the Put.
func (p *Put) Discard() error {
	p.b.Clear()
	return nil
}

type Take struct {
	p     *Pipe
	b     *levigo.WriteBatch
	taken map[uint64]struct{}
	mutex *sync.Mutex
	it    *levigo.Iterator
}

// Take gets an item from the pipe.
func (t *Take) Take() ([]byte, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if firstKey := t.p.firstKey(); firstKey != nil {
		t.it.Seek(firstKey)
	} else {
		t.it.SeekToFirst()
	}
	m := 0
	for t.it.Valid() {
		m++
		k := t.it.Key()
		v := t.it.Value()
		id, n := binary.Uvarint(k)
		if n <= 0 {
			log.Fatalln("couldn't parse key: " + string(k))
		}
		if _, ok := t.p.taken[id]; !ok {
			t.p.mark(id)
			t.b.Delete(k)
			return v, nil
		}
		t.taken[id] = struct{}{}
		t.it.Next()
	}
	return nil, nil
}

// Commit removes the taken items from the pipe.
func (t *Take) Commit() error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	wo.SetSync(true)
	err := t.p.db.Write(wo, t.b)
	for k := range t.taken {
		t.p.unmark(k)
		t.p.take(k)
	}
	t.taken = make(map[uint64]struct{})
	return err
}

// Rollback returns the items to the queue.
func (t *Take) Rollback() error {
	for k := range t.taken {
		t.p.unmark(k)
	}
	return nil
}

// Close closes the transaction.
func (t *Take) Close() error {
	t.b.Close()
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
	Rollback() error
	Close() error
}
