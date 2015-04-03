leviq
=====

leviq is a persistent transactional queue, originally built on top of [levigo](https://github.com/jmhodges/levigo) and [gosnow](https://github.com/sdming/gosnow), but now also supports [Bolt](https://github.com/boltdb/bolt) and [goleveldb](https://github.com/syndtr/goleveldb).

Documentation is available on [GoDoc](https://godoc.org/github.com/johnsto/leviq) and [Go Walker](https://gowalker.org/github.com/johnsto/leviq).

This code is not in production use, so tread carefully.

## Example

```
package main

import (
	"time"

	"github.com/johnsto/leviq/backend/goleveldb"
)

func main() {
	// Open the on-disk database
	db, _ := goleveldb.Open("db.db")
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
leviq currently provides backends for the following LevelDB (or LevelDB-like) databases:

### [goleveldb](https://github.com/syndtr/goleveldb)
Uses the Go-native implementation of LevelDB. Currently the best-performing of all three backends, and doesn't require any external libraries. Use this backend unless you have a good reason to use another.

### [levigo](https://github.com/jmhodges/levigo)
This backend uses the Go bindings to the native Level DB libraries, and therefore has a third-party dependency. Performance is similar to goleveldb (or slightly lower, in my experience).


### [Bolt](https://github.com/boltdb/bolt)
Currently slower than either of the two LevelDB-based backends, but growing in popularity.

