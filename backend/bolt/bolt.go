package bolt

import "github.com/johnsto/leviq/backend"
import "github.com/boltdb/bolt"

type BoltDB struct {
	db *bolt.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

func Open(path string) (*BoltDB, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	return NewBoltDB(db), nil
}

func NewBoltDB(db *bolt.DB) *BoltDB {
	return &BoltDB{db}
}

func (db *BoltDB) Batch() backend.Batch {
	return &BoltBatch{
		db: db,
		tx: db.db.Begin(true),
	}
}

func (db *BoltDB) Iterator() backend.Iterator {
	ro := levigo.NewReadOptions()
	defer ro.Close()

	it := db.db.NewIterator(ro)
	return &BoltIterator{
		db: db,
		it: it,
	}
}

func (db *BoltDB) Get(k []byte) ([]byte, error) {
	return db.Get(k), nil
}

func (db *BoltDB) Close() {
	db.db.Close()
}

type BoltBatch struct {
	db *BoltDB
	tx *bolt.Tx
}

func (b *BoltBatch) Put(k, v []byte) {
	b.batch.Put(k, v)
}

func (b *BoltBatch) Delete(k []byte) {
	b.batch.Delete(k)
}

func (b *BoltBatch) Write() error {
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	defer wo.Close()
	return b.db.db.Write(wo, b.batch)
}

func (b *BoltBatch) Clear() {
	b.batch.Clear()
}

func (b *BoltBatch) Close() {
	b.batch.Close()
}

type BoltIterator struct {
	db *BoltDB
	it *levigo.Iterator
}

func (i BoltIterator) Seek(k []byte) {
	i.it.Seek(k)
}

func (i BoltIterator) SeekToFirst() {
	i.it.SeekToFirst()
}

func (i BoltIterator) Next() {
	i.it.Next()
}

func (i BoltIterator) Valid() bool {
	return i.it.Valid()
}

func (i BoltIterator) Key() []byte {
	return i.it.Key()
}

func (i BoltIterator) Close() {
	i.it.Close()
}
