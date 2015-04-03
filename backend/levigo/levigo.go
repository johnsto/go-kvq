package levigo

import (
	"bytes"
	"github.com/jmhodges/levigo"
	"github.com/johnsto/leviq"
	"github.com/johnsto/leviq/backend"
)

type DB struct {
	levigoDB *levigo.DB
}

func Open(path string) (*leviq.DB, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return New(db), nil
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

func New(db *levigo.DB) *leviq.DB {
	return leviq.NewDB(&DB{db})
}

func (db *DB) Bucket(name string) (backend.Bucket, error) {
	return &Bucket{
		db: db,
		ns: []byte(name),
	}, nil
}

func (db *DB) Close() {
	db.levigoDB.Close()
}

type Bucket struct {
	db *DB
	ns []byte
}

func (q *Bucket) ForEach(fn func(k, v []byte) error) error {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	ro.SetFillCache(false)
	it := q.db.levigoDB.NewIterator(ro)
	defer it.Close()

	for it.Seek(q.ns); it.Valid(); it.Next() {
		kk, v := it.Key(), it.Value()

		if !bytes.HasPrefix(kk, q.ns) {
			// Stop iterating if exceeded namespace
			break
		}

		k := kk[len(q.ns):]
		if err := fn(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (q *Bucket) Batch(fn func(backend.Batch) error) error {
	batch := NewBatch(q)
	defer batch.Close()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Write()
}

func (q *Bucket) Get(k []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	kk := append(q.ns[:], k...)
	return q.db.levigoDB.Get(ro, kk)
}

func (q *Bucket) Clear() error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()
	err := q.ForEach(func(k, _ []byte) error {
		wb.Delete(k)
		return nil
	})
	if err != nil {
		return err
	}
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	defer wo.Close()
	return q.db.levigoDB.Write(wo, wb)
}

type Batch struct {
	levigoDB         *levigo.DB
	levigoWriteBatch *levigo.WriteBatch
	ns               []byte
}

func NewBatch(q *Bucket) *Batch {
	wb := levigo.NewWriteBatch()
	return &Batch{
		ns:               q.ns,
		levigoDB:         q.db.levigoDB,
		levigoWriteBatch: wb,
	}
}

func (b *Batch) Put(k, v []byte) error {
	kk := append(b.ns[:], k...)
	b.levigoWriteBatch.Put(kk, v)
	return nil
}

func (b *Batch) Delete(k []byte) error {
	kk := append(b.ns[:], k...)
	b.levigoWriteBatch.Delete(kk)
	return nil
}

func (b *Batch) Write() error {
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	defer wo.Close()
	return b.levigoDB.Write(wo, b.levigoWriteBatch)
}

func (b *Batch) Clear() {
	b.levigoWriteBatch.Clear()
}

func (b *Batch) Close() {
	b.levigoWriteBatch.Close()
}
