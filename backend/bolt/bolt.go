package bolt

import (
	"github.com/boltdb/bolt"
	"github.com/johnsto/leviq"
	"github.com/johnsto/leviq/backend"
	"os"
)

type DB struct {
	boltDB *bolt.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return os.RemoveAll(path)
}

func Open(path string) (*leviq.DB, error) {
	db, err := bolt.Open(path, 0777, nil)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

func New(db *bolt.DB) *leviq.DB {
	return leviq.NewDB(&DB{db})
}

func (db *DB) Bucket(name string) (backend.Bucket, error) {
	return &Bucket{
		db:   db,
		name: name,
	}, nil
}

func (db *DB) Close() {
	db.boltDB.Close()
}

type Bucket struct {
	db   *DB
	name string
}

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

func (q *Bucket) ForEach(fn func(k, v []byte) error) error {
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		return bucket.ForEach(fn)
	})
}

func (q *Bucket) Get(k []byte) ([]byte, error) {
	var v []byte
	return v, q.db.boltDB.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		v = bucket.Get(k)
		return nil
	})
}

func (q *Bucket) Clear() error {
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(q.name))
	})
}

type Batch struct {
	bucket *bolt.Bucket
}

func (b *Batch) Put(k, v []byte) error {
	return b.bucket.Put(k, v)
}

func (b *Batch) Delete(k []byte) error {
	return b.bucket.Delete(k)
}

func (b *Batch) Close() {
}
