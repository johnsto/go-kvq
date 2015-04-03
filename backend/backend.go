package backend

type DB interface {
	Batch() Batch
	Iterator() Iterator
	Get(k []byte) (v []byte, err error)
	Close()
}

type Batch interface {
	Put(k, v []byte)
	Delete(k []byte)
	Write() error
	Clear()
	Close()
}

type Iterator interface {
	Seek(k []byte)
	SeekToFirst()
	Next()
	Valid() bool
	Key() []byte
	Close()
}
