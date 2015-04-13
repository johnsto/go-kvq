package leviq

import (
	"github.com/johnsto/leviq/backend"
	"github.com/johnsto/leviq/backend/goleveldb"
)

// DB wraps the backend being used.
type DB struct {
	backend.DB
}

func Open(path string) (*DB, error) {
	db, err := goleveldb.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

func Destroy(path string) error {
	return goleveldb.Destroy(path)
}

// NewDB creates a new DB instance from a backend database.
func NewDB(db backend.DB) *DB {
	return &DB{db}
}

// Queue opens a queue within the given namespace (key prefix), whereby keys
// are prefixed with the namespace value and a NUL byte, followed by the
// ID of the queued item.
func (db *DB) Queue(namespace string) (*Queue, error) {
	return NewQueue(db.DB, namespace, nil)
}
