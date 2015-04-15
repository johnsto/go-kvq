go-kvq
======

**`kvq` is an implementation of a persistent, transactional, non-distributed 
queue built on top of standard K/V databases, namely LevelDB.**

Values are stored in the underlying database with keys generated based on
insertion time (using [gosnow](https://github.com/sdming/gosnow)), relying on
the native bytewise key ordering provided by LevelDB derivatives. Multiple 
queues may be created within a database, but may not be shared by different
processes (multiple threads may share a queue safely, however.)

Documentation is available on [GoDoc](https://godoc.org/github.com/johnsto/go-kvq) and [Go Walker](https://gowalker.org/github.com/johnsto/go-kvq).

This code is not in production use, so tread carefully.

## Example

```
package main

import (
	"time"

	"github.com/johnsto/go-kvq"
)

func main() {
	// Open the on-disk database
	db, _ := kvq.Open("db.db")
	defer db.Close()

	// Get a named queue
	queue, _ := db.Queue("welcome")

	// Write some data to the queue
	txn := queue.Transaction()
	defer txn.Close()
	txn.Put([]byte("hello"))
	txn.Put([]byte("world"))
	txn.Commit()

	// Read the data from the queue
	values, _ := txn.TakeN(2, time.Second)
	for _, value := range values {
		print(string(value) + "\n")
	}
	txn.Commit()
}

```

## Backends
`kvq` currently provides backends for the following LevelDB (or LevelDB-like)
databases:

### [goleveldb](https://github.com/syndtr/goleveldb)
Uses the Go-native implementation of LevelDB. Currently the best-performing of
all three backends, and doesn't require any external libraries. Use this
backend unless you have a good reason to use another.

### [levigo](https://github.com/jmhodges/levigo)
This backend uses the Go bindings to the native Level DB libraries, and
therefore has a third-party dependency. Performance is similar to goleveldb
(or slightly lower, in my experience).

### [Bolt](https://github.com/boltdb/bolt)
Currently slower than either of the two LevelDB-based backends, but included
for completeness.

### Adding another backend
Adding support for another backend is as simple as implementing the interfaces
defined in `github.com/johnsto/go-kvq/backend`. See the provided
implementations for examples.
