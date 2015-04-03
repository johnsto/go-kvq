package levigo

import "github.com/johnsto/leviq/backend"
import "github.com/jmhodges/levigo"

type LevigoDB struct {
	db *levigo.DB
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

func (db *LevigoDB) Batch() backend.Batch {
	return &LevigoBatch{
		db:    db,
		batch: levigo.NewWriteBatch(),
	}
}

func (db *LevigoDB) Iterator() backend.Iterator {
	ro := levigo.NewReadOptions()
	defer ro.Close()

	it := db.db.NewIterator(ro)
	return &LevigoIterator{
		db: db,
		it: it,
	}
}

func (db *LevigoDB) Get(k []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	return db.db.Get(ro, k)
}

func (db *LevigoDB) Close() {
	db.db.Close()
}

type LevigoBatch struct {
	db    *LevigoDB
	batch *levigo.WriteBatch
}

func (b *LevigoBatch) Put(k, v []byte) {
	b.batch.Put(k, v)
}

func (b *LevigoBatch) Delete(k []byte) {
	b.batch.Delete(k)
}

func (b *LevigoBatch) Write() error {
	wo := levigo.NewWriteOptions()
	wo.SetSync(true)
	defer wo.Close()
	return b.db.db.Write(wo, b.batch)
}

func (b *LevigoBatch) Clear() {
	b.batch.Clear()
}

func (b *LevigoBatch) Close() {
	b.batch.Close()
}

type LevigoIterator struct {
	db *LevigoDB
	it *levigo.Iterator
}

func (i LevigoIterator) Seek(k []byte) {
	i.it.Seek(k)
}

func (i LevigoIterator) SeekToFirst() {
	i.it.SeekToFirst()
}

func (i LevigoIterator) Next() {
	i.it.Next()
}

func (i LevigoIterator) Valid() bool {
	return i.it.Valid()
}

func (i LevigoIterator) Key() []byte {
	return i.it.Key()
}

func (i LevigoIterator) Close() {
	i.it.Close()
}
