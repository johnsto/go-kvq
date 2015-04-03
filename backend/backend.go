package backend

type DB interface {
	Queue(name string) Queue
	Close()
}

type Queue interface {
	ForEach(fn func(k, v []byte) error) error
	Batch(fn func(Batch) error) error
	Get(k []byte) ([]byte, error)
	Clear() error
}

type Batch interface {
	Put(k, v []byte)
	Delete(k []byte)
	Close()
}
