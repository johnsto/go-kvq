package main

import (
	"encoding/binary"
	"log"
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/sdming/gosnow"
)

var snow *gosnow.SnowFlake

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
	start []byte
	it    *levigo.Iterator
}

func NewPipe(path string) (*Pipe, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	ro := levigo.NewReadOptions()

	return &Pipe{
		db:    db,
		taken: make(map[uint64]struct{}),
		mutex: &sync.Mutex{},
		it:    db.NewIterator(ro),
	}, nil
}

func (p *Pipe) Close() {
	p.db.Close()
}

func (p *Pipe) Put() Put {
	return Put{
		p:     p,
		b:     levigo.NewWriteBatch(),
		puts:  make(map[uint64]struct{}),
		first: 0,
	}
}

func (p *Pipe) Take() Take {
	return Take{
		p:     p,
		b:     levigo.NewWriteBatch(),
		taken: make(map[uint64]struct{}),
		mutex: &sync.Mutex{},
	}
}

func (p *Pipe) take() (k, v []byte, err error) {
	return nil, nil, nil
}

func (p *Pipe) mark(k []byte) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	id, err := binary.Uvarint(k)
	if err != nil {
		log.Fatalln("couldn't translate key to uint64:", err)
	}
	p.taken[id] = struct{}{}
}

func (p *Pipe) unmark(k []byte) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	id, err := binary.Uvarint(k)
	if err != nil {
		log.Fatalln("couldn't translate key to uint64:", err)
	}
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
	err := p.p.db.Write(wo, p.b)
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
}

// Take gets an item from the pipe.
func (t *Take) Take() ([]byte, error) {
	return nil, nil
}

// Commit removes the taken items from the pipe.
func (t *Take) Commit() error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	err := t.p.db.Write(wo, t.b)
	for k := range t.taken {
		t.p.Unflag(k)
	}
	return err
}

// Rollback returns the items to the queue.
func (t *Take) Rollback() error {
	for k := range t.taken {
		t.p.Unflag(k)
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
