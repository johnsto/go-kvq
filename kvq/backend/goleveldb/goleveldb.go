package goleveldb

import (
	"fmt"
	"os"

	"github.com/johnsto/go-kvq/kvq/backend"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// DB encapsulates a LevelDB instance.
type DB struct {
	levelDB *leveldb.DB
}

// Open creates or opens an existing DB at the given path.
func Open(path string) (backend.DB, error) {
	levelDB, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &DB{levelDB}, nil
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return os.RemoveAll(path)
}

// New returns a DB from the given LevelDB instance.
func New(db *leveldb.DB) backend.DB {
	return &DB{db}
}

// NewMem creates a new DB backed by memory only (i.e. not persistent)
func NewMem() (backend.DB, error) {
	storage := storage.NewMemStorage()
	levelDB, err := leveldb.Open(storage, nil)
	if err != nil {
		return nil, err
	}
	return &DB{levelDB}, nil
}

// Bucket returns a queue in the given namespace.
func (db *DB) Bucket(name string) (backend.Bucket, error) {
	// Prefix namespace with length to avoid conflicts between namespaces
	// (e.g. "test" and "testing")
	if len(name) > 0xff {
		return nil, fmt.Errorf("namespace must be <255 chars")
	}

	n := byte(len(name))
	ns := append([]byte{n}, []byte(name)...)

	return &Bucket{
		db: db,
		ns: ns,
	}, nil
}

// Close closes the database and releases any resources.
func (db *DB) Close() {
	db.levelDB.Close()
}

// Bucket represents a goleveldb-backed queue, where each key is prefixed by
// the given namespace. All batch writes are synced by default.
type Bucket struct {
	db *DB
	ns []byte
}

// ForEach iterates through keys in the queue. If the iteration function
// returns a non-nil error, iteration stops and the error is returned to
// the caller.
func (q *Bucket) ForEach(fn func(k, v []byte) error) error {
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

// Batch enacts a number of operations in one atomic go. If the batch
// function returns a non-nil error, the batch is discarded and the error
// is returned to the caller. If the batch function returns nil, the batch
// is committed to the queue.
func (q *Bucket) Batch(fn func(backend.Batch) error) error {
	b := &leveldb.Batch{}
	batch := &Batch{
		ns:         q.ns,
		levelDB:    q.db.levelDB,
		levelBatch: b,
	}
	defer batch.Close()

	if err := fn(batch); err != nil {
		return err
	}

	wo := &opt.WriteOptions{Sync: true}
	return q.db.levelDB.Write(b, wo)
}

// Get returns the value stored at key `k`.
func (q *Bucket) Get(k []byte) ([]byte, error) {
	kk := append(q.ns[:], k...)
	vv, err := q.db.levelDB.Get(kk, nil)
	if err == leveldb.ErrNotFound {
		return nil, backend.ErrKeyNotFound
	}
	return vv, err
}

// Clear removes all items from this queue.
func (q *Bucket) Clear() error {
	keyRange := util.BytesPrefix(q.ns)
	it := q.db.levelDB.NewIterator(keyRange, nil)

	b := &leveldb.Batch{}

	for it.Next() {
		kk := it.Key()
		k := kk[len(q.ns):]
		b.Delete(k)
	}

	wo := &opt.WriteOptions{Sync: true}
	return q.db.levelDB.Write(b, wo)
}

// Batch represents a set of put/delete operations to perform on a Bucket.
type Batch struct {
	levelDB    *leveldb.DB
	levelBatch *leveldb.Batch
	ns         []byte
}

// Put sets the key `k` to value `v`.
func (b *Batch) Put(k, v []byte) error {
	kk := append(b.ns[:], k...)
	b.levelBatch.Put(kk, v)
	return nil
}

// Delete deletes the key `k`.
func (b *Batch) Delete(k []byte) error {
	kk := append(b.ns[:], k...)
	b.levelBatch.Delete(kk)
	return nil
}

// Close discards this batch.
func (b *Batch) Close() {
	b.levelBatch.Reset()
}
