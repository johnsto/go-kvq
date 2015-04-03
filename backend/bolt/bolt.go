package bolt

import "github.com/johnsto/leviq/backend"
import "github.com/boltdb/bolt"
import "log"
import "os"

type BoltDB struct {
	boltDB *bolt.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return os.RemoveAll(path)
}

func Open(path string) (*BoltDB, error) {
	db, err := bolt.Open(path, 0777, nil)
	if err != nil {
		return nil, err
	}
	return NewBoltDB(db), nil
}

func NewBoltDB(db *bolt.DB) *BoltDB {
	return &BoltDB{db}
}

func (db *BoltDB) Queue(name string) backend.Queue {
	return &BoltQueue{
		db:   db,
		name: name,
	}
}

func (db *BoltDB) Close() {
	db.boltDB.Close()
}

type BoltQueue struct {
	db   *BoltDB
	name string
}

func (q *BoltQueue) ForEach(fn func(k, v []byte) error) error {
	return q.db.boltDB.View(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		return bucket.ForEach(fn)
	})
}

func (q *BoltQueue) Batch(fn func(backend.Batch) error) error {
	log.Println("nbatch!")
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		log.Println(tx.Writable())
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		batch := &BoltBatch{
			bucket: bucket,
		}
		return fn(batch)
	})
}

func (q *BoltQueue) Get(k []byte) ([]byte, error) {
	var v []byte
	return v, q.db.boltDB.View(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(q.name))
		if err != nil {
			return err
		}
		v = bucket.Get(k)
		return nil
	})
}

func (q *BoltQueue) Clear() error {
	return q.db.boltDB.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(q.name))
	})
}

type BoltBatch struct {
	bucket *bolt.Bucket
}

func (b *BoltBatch) Put(k, v []byte) error {
	return b.bucket.Put(k, v)
}

func (b *BoltBatch) Delete(k []byte) error {
	return b.bucket.Delete(k)
}

func (b *BoltBatch) Close() {
}
