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
	taken map[[]byte]struct{}
	mutex *sync.Mutex
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
		taken: make(map[[]byte]struct{}),
		mutex: &sync.Mutex{},
		it:    db.NewIterator(ro),
	}, nil
}

func (p *Pipe) Close() {
	p.db.Close()
}

func (p *Pipe) Put() Put {
	return Put{
		p: p,
		b: levigo.NewWriteBatch(),
	}
}

func (p *Pipe) Take() Take {
	return Take{
		p:     p,
		b:     levigo.NewWriteBatch(),
		taken: make(map[[]byte]struct{}),
		mutex: &sync.Mutex{},
	}
}

func (p *Pipe) take() (k, v []byte, err error) {
	return nil, nil, nil
}

func (p *Pipe) mark(k []byte) {
	p.mutex.Lock()
	p.taken[b] = struct{}{}
	defer p.mutex.Unlock()
}

func (p *Pipe) unmark(k []byte) {
	p.mutex.Lock()
	delete(p.taken[b])
	defer p.mutex.Unlock()
}

type Put struct {
	p *Pipe
	b *levigo.WriteBatch
}

// Put inserts the data into the pipe.
func (p *Put) Put(v []byte) error {
	id, err := snow.Next()
	if err != nil {
		return err
	}
	k := make([]byte, 16)
	binary.PutUvarint(k, id)
	p.b.Put(k, v)
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
	taken map[[]byte]struct{}
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
	err := p.p.db.Write(wo, p.b)
	for k := range t.taken {
		t.db.Unflag(k)
	}
	return err
}

// Rollback returns the items to the queue.
func (t *Take) Rollback() error {
	for k := range t.taken {
		t.db.Unflag(k)
	}
	return nil
}

// Close closes the transaction.
func (t *Take) Close() error {
	p.b.Close()
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
