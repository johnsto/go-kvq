package leviq

import (
	"sync"

	"github.com/jmhodges/levigo"
	"github.com/johnsto/leviq/internal"
)

// DB is little more than a wrapper around a Levigo DB
type DB struct {
	db *levigo.DB
}

// Destroy destroys the DB at the given path.
func Destroy(path string) error {
	return levigo.DestroyDatabase(path, levigo.NewOptions())
}

// Open opens the DB at the given path.
func Open(path string, opts *levigo.Options) (*DB, error) {
	if opts == nil {
		opts = levigo.NewOptions()
		opts.SetCreateIfMissing(true)
	}

	db, err := levigo.Open(path, opts)
	if err != nil {
		return nil, err
	}
	return &DB{
		db: db,
	}, nil
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

// Close closes the queue.
func (db *DB) Close() {
	db.db.Close()
}
