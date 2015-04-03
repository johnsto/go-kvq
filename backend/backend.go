package backend

type Open func(path string) (DB, error)
type Destroy func(path string) error

type DB interface {
	Queue(name string) (Queue, error)
	Close()
}

type Queue interface {
	ForEach(fn func(k, v []byte) error) error
	Batch(fn func(Batch) error) error
	Get(k []byte) ([]byte, error)
	Clear() error
}

type Batch interface {
	Put(k, v []byte) error
	Delete(k []byte) error
	Close()
}
