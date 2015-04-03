package goleveldb

import (
	"os"

	"github.com/johnsto/leviq/backend"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type DB struct {
	levelDB *leveldb.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return os.RemoveAll(path)
}

func Open(path string) (*DB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

func NewMemDB() (*DB, error) {
	storage := storage.NewMemStorage()
	db, err := leveldb.Open(storage, nil)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func NewDB(db *leveldb.DB) *DB {
	return &DB{db}
}

func (db *DB) Queue(name string) (backend.Queue, error) {
	return &Queue{
		db: db,
		ns: []byte(name),
	}, nil
}

func (db *DB) Close() {
	db.levelDB.Close()
}

type Queue struct {
	db *DB
	ns []byte
}

func (q *Queue) ForEach(fn func(k, v []byte) error) error {
	keyRange := util.BytesPrefix(q.ns)
	it := q.db.levelDB.NewIterator(keyRange, nil)

	for it.Next() {
		kk, v := it.Key(), it.Value()
		k := kk[len(q.ns):]
		if err := fn(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (q *Queue) Batch(fn func(backend.Batch) error) error {
	batch := NewBatch(q)
	defer batch.Close()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Write()
}

func (q *Queue) Get(k []byte) ([]byte, error) {
	kk := append(q.ns[:], k...)
	return q.db.levelDB.Get(kk, nil)
}

func (q *Queue) Clear() error {
	keyRange := util.BytesPrefix(q.ns)
	it := q.db.levelDB.NewIterator(keyRange, nil)

	b := &leveldb.Batch{}

	for it.Next() {
		kk := it.Key()
		k := kk[len(q.ns):]
		b.Delete(k)
	}

	return q.db.levelDB.Write(b, nil)
}

type Batch struct {
	levelDB    *leveldb.DB
	levelBatch *leveldb.Batch
	ns         []byte
}

func NewBatch(q *Queue) *Batch {
	return &Batch{
		ns:         q.ns,
		levelDB:    q.db.levelDB,
		levelBatch: &leveldb.Batch{},
	}
}

func (b *Batch) Put(k, v []byte) error {
	kk := append(b.ns[:], k...)
	b.levelBatch.Put(kk, v)
	return nil
}

func (b *Batch) Delete(k []byte) error {
	kk := append(b.ns[:], k...)
	b.levelBatch.Delete(kk)
	return nil
}

func (b *Batch) Write() error {
	return b.levelDB.Write(b.levelBatch, nil)
}

func (b *Batch) Clear() {
	b.levelBatch.Reset()
}

func (b *Batch) Close() {
	b.levelBatch.Reset()
}
