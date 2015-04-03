package backend

// Open opens a database at the given path.
type Open func(path string) (DB, error)

// Destroy destroys the database at the given path.
type Destroy func(path string) error

// DB represents a set of namespaced queues.
type DB interface {
	// Bucket returns a bucket in the given namespace.
	Bucket(name string) (Bucket, error)
	// Close closes the database and releases any resources.
	Close()
}

// Bucket represents a set of keys within a DB.
type Bucket interface {
	// ForEach iterates through keys in the bucket. If the iteration function
	// returns a non-nil error, iteration stops and the error is returned to
	// the caller.
	ForEach(fn func(k, v []byte) error) error
	// Batch enacts a number of operations in one atomic go. If the batch
	// function returns a non-nil error, the batch is discarded and the error
	// is returned to the caller. If the batch function returns nil, the batch
	// is committed to the queue.
	Batch(fn func(Batch) error) error
	// Get returns the value stored at key `k`.
	Get(k []byte) ([]byte, error)
	// Clear removes all items from this bucket.
	Clear() error
}

// Batch represents a set of put/delete operations to perform on a Queue.
type Batch interface {
	// Put sets the key `k` to value `v`.
	Put(k, v []byte) error
	// Delete deletes the key `k`.
	Delete(k []byte) error
	// Close discards this batch.
	Close()
}
