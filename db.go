package leviq

import (
	"sync"

	"github.com/johnsto/leviq/backend"
	"github.com/johnsto/leviq/internal"
)

type DB struct {
	backend.DB
}

func NewDB(db backend.DB) *DB {
	return &DB{db}
}

// Queue opens a queue within the given namespace (key prefix), whereby keys
// are prefixed with the namespace value and a NUL byte, followed by the
// ID of the queued item.
func (db *DB) Queue(namespace string) (*Queue, error) {
	var ns []byte
	if namespace != "" {
		ns = append([]byte(namespace), 0)
	}
	queue := &Queue{
		ns:    ns,
		db:    db,
		mutex: &sync.Mutex{},
		ids:   internal.NewIDHeap(),
		c:     make(chan struct{}, MaxQueue),
	}
	if err := queue.init(); err != nil {
		return nil, err
	}
	return queue, nil
}
