package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/johnsto/go-kvq/kvq"
	"github.com/johnsto/go-kvq/kvq/backend"
	"os"
)

// DB encapsulates a Bolt DB instance.
type DB struct {
	boltDB *bolt.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return os.RemoveAll(path)
}

// Open returns a DB instance from the Bolt database at the given path, using
// default parameters.
func Open(path string) (*kvq.DB, error) {
	db, err := bolt.Open(path, 0777, nil)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

// New returns a DB from the given Bolt DB instance.
func New(db *bolt.DB) *kvq.DB {
	return kvq.NewDB(&DB{db})
}

// Bucket returns a queue in the given namespace.
func (db *DB) Bucket(name string) (backend.Bucket, error) {
	return &Bucket{
		db:   db,
		name: name,
	}, nil
}

// Close closes the bolt database and releases any resources.
func (db *DB) Close() {
	db.boltDB.Close()
}

// Bucket represents a set of keys within a DB.
type Bucket struct {
	db   *DB
	name string
}

// ForEach iterates through keys in the bucket. If the iteration function
// returns a non-nil error, iteration stops and the error is returned to
// the caller.
func (q *Bucket) ForEach(fn func(k, v []byte) error) error {
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		return bucket.ForEach(fn)
	})
}

// Batch enacts a number of operations in one atomic go. If the batch
// function returns a non-nil error, the batch is discarded and the error
// is returned to the caller. If the batch function returns nil, the batch
// is committed to the queue.
func (q *Bucket) Batch(fn func(backend.Batch) error) error {
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		batch := &Batch{
			bucket: bucket,
		}
		return fn(batch)
	})
}

// Get returns the value stored at key `k`.
func (q *Bucket) Get(k []byte) ([]byte, error) {
	var v []byte
	return v, q.db.boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		v = bucket.Get(k)
		if v == nil {
			return backend.ErrKeyNotFound
		}
		return nil
	})
}

// Clear removes all items from this bucket.
func (q *Bucket) Clear() error {
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(q.name))
	})
}

// Batch represents a set of put/delete operations to perform on a Queue.
type Batch struct {
	bucket *bolt.Bucket
}

// Put sets the key `k` to value `v`.
func (b *Batch) Put(k, v []byte) error {
	return b.bucket.Put(k, v)
}

// Delete deletes the key `k`.
func (b *Batch) Delete(k []byte) error {
	return b.bucket.Delete(k)
}

// Close discards this batch.
func (b *Batch) Close() {
}
