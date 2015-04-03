package levigo

import (
	"bytes"
)
import "github.com/johnsto/leviq/backend"
import "github.com/jmhodges/levigo"

type LevigoDB struct {
	levigoDB *levigo.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

func Open(path string) (*LevigoDB, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return NewLevigoDB(db), nil
}

func NewLevigoDB(db *levigo.DB) *LevigoDB {
	return &LevigoDB{db}
}

func (db *LevigoDB) Queue(name string) backend.Queue {
	return &LevigoQueue{
		db: db,
		ns: []byte(name),
	}
}

func (db *LevigoDB) Close() {
	db.levigoDB.Close()
}

type LevigoQueue struct {
	db *LevigoDB
	ns []byte
}

func (q *LevigoQueue) ForEach(fn func(k, v []byte) error) error {
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

func (q *LevigoQueue) Batch(fn func(backend.Batch) error) error {
	batch := NewLevigoBatch(q)
	defer batch.Close()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Write()
}

func (q *LevigoQueue) Get(k []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	kk := append(q.ns[:], k...)
	return q.db.levigoDB.Get(ro, kk)
}

func (q *LevigoQueue) Clear() error {
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

type LevigoBatch struct {
	levigoDB         *levigo.DB
	levigoWriteBatch *levigo.WriteBatch
	ns               []byte
}

func NewLevigoBatch(q *LevigoQueue) *LevigoBatch {
	wb := levigo.NewWriteBatch()
	return &LevigoBatch{
		ns:               q.ns,
		levigoDB:         q.db.levigoDB,
		levigoWriteBatch: wb,
	}
}

func (b *LevigoBatch) Put(k, v []byte) {
	kk := append(b.ns[:], k...)
	b.levigoWriteBatch.Put(kk, v)
}

func (b *LevigoBatch) Delete(k []byte) {
	kk := append(b.ns[:], k...)
	b.levigoWriteBatch.Delete(kk)
}

func (b *LevigoBatch) Write() error {
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	defer wo.Close()
	return b.levigoDB.Write(wo, b.levigoWriteBatch)
}

func (b *LevigoBatch) Clear() {
	b.levigoWriteBatch.Clear()
}

func (b *LevigoBatch) Close() {
	b.levigoWriteBatch.Close()
}
