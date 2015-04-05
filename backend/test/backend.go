package test

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/johnsto/leviq"
	"github.com/stretchr/testify/assert"
)

type OpenDB func(path string) (*leviq.DB, error)
type DestroyDB func(path string) error

// TestInit ensures that data are loaded again correctly from disk.
func TestInit(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-init.db"
	err := destroy(path)

	// Open initial DB
	db, err := open(path)
	if err != nil {
		log.Fatalln(err)
	}
	q, err := db.Queue("test")
	assert.NoError(t, err)

	table := []string{"a", "b", "c"}

	// Put initial values
	tx := q.Transaction()
	for _, value := range table {
		tx.Put([]byte(value))
	}
	assert.NoError(t, tx.Commit())
	assert.NoError(t, tx.Close())

	// Re-open DB
	db.Close()
	db, err = open(path)
	assert.NoError(t, err)

	q, err = db.Queue("test")
	assert.NoError(t, err)

	// Read expected values
	tx = q.Transaction()
	for _, value := range table {
		v, err := tx.Take()
		assert.NoError(t, err)
		assert.Equal(t, []byte(value), v)
	}
	assert.NoError(t, tx.Commit())

	// Re-open DB
	db.Close()
	db, err = open(path)
	assert.NoError(t, err)

	q, err = db.Queue("test")
	assert.NoError(t, err)

	// Ensure read values are now gone
	tx = q.Transaction()
	v, err := tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	assert.NoError(t, tx.Close())
	db.Close()
}

// TestQueueSingle tests a batch of puts and takes in a single transaction
func TestQueueSingle(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-queue-single.db"

	err := destroy(path)
	db, err := open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.NoError(t, err)
	if err := q.Clear(); err != nil {
		log.Fatalln(err)
	}

	// Create test data
	table := map[string]bool{}
	for i := 0; i < 100; i++ {
		k := strconv.Itoa(i)
		table[k] = true
	}

	// Write data
	tx := q.Transaction()
	for k := range table {
		tx.Put([]byte(k))
	}
	tx.Commit()

	// Read data
	rx := q.Transaction()
	vs, err := rx.TakeN(50, 10*time.Second)
	assert.NoError(t, err)
	for _, v := range vs {
		assert.True(t, table[string(v)])
		delete(table, string(v))
	}

	for len(table) > 0 {
		v, err := rx.Take()
		assert.NoError(t, err)
		assert.True(t, table[string(v)])
		delete(table, string(v))
	}
	rx.Commit()

	// Verify there are no more items in the queue
	rx = q.Transaction()
	defer rx.Close()
	v, err := rx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
}

// TestQueueMulti tests a series of puts/takes in a number of transactions
func TestQueueMulti(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-queue-multi.db"

	err := destroy(path)
	db, err := open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.NoError(t, err)
	if err := q.Clear(); err != nil {
		log.Fatalln(err)
	}

	table := []string{"a", "b", "c"}

	// Put values
	for _, v := range table {
		tx := q.Transaction()
		tx.Put([]byte(v))
		tx.Commit()
	}

	// Take values
	for _, exp := range table {
		rx := q.Transaction()
		act, err := rx.Take()
		assert.NoError(t, err)
		assert.Equal(t, exp, string(act))
		rx.Commit()
	}
}

// TestQueueOrdered tests that items come out of the queue in the correct
// order.
func TestQueueOrdered(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-queue-ordered.db"

	err := destroy(path)
	db, err := open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.NoError(t, err)
	if err := q.Clear(); err != nil {
		log.Fatalln(err)
	}

	// Put values
	tx := q.Transaction()
	for i := byte(0); i < 100; i++ {
		tx.Put([]byte{i})
		// Artificial sleep to guarantee new gosnow ID
		time.Sleep(time.Millisecond * 2)
	}
	tx.Commit()

	// Take values
	rx := q.Transaction()
	for i := byte(0); i < 100; i++ {
		act, err := rx.Take()
		assert.NoError(t, err)
		assert.Equal(t, []byte{i}, act)
	}
	rx.Commit()
}

// TestQueueThreaded puts and takes items from a number of simultaneous
// goroutines.
func TestQueueThreaded(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-queue-threaded.db"

	err := destroy(path)
	db, err := open(path)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.NoError(t, err)
	if err := q.Clear(); err != nil {
		log.Fatalln(err)
	}

	routines := 4
	n := 1000

	inp := make(chan string)
	outp := make(chan string)
	quit := make(chan bool)
	wg := &sync.WaitGroup{}

	for i := 0; i < routines; i++ {
		// Fill queue with items from input channel
		wg.Add(1)
		go func() {
			m := 0
			for s := range inp {
				tx := q.Transaction()
				defer tx.Close()
				tx.Put([]byte(s))
				tx.Commit()
				m++
			}
			wg.Done()
		}()

		// Pull items from queue and put into output channel
		wg.Add(1)
		go func() {
			m := 0
			active := true
			for active {
				select {
				case <-quit:
					active = false
					break
				default:
					rx := q.Transaction()
					defer rx.Close()
					v, err := rx.Take()
					assert.NoError(t, err)
					if v == nil {
						time.Sleep(50 * time.Millisecond)
					} else {
						m++
						outp <- string(v)
						rx.Commit()
					}
					rx.Close()
				}
			}
			wg.Done()
		}()
	}

	// Ensure num items put into input queue is same as output queue

	m := make(map[string]bool)

	for i := 0; i < n; i++ {
		s := strconv.Itoa(i)
		m[s] = true
		inp <- s
	}
	close(inp)

	for i := 0; i < n; i++ {
		s := <-outp
		assert.True(t, m[s])
		m[s] = false
	}
	select {
	case <-outp:
		assert.Fail(t, "items left in queue!")
	default:
	}
	close(outp)

	for i := 0; i < routines; i++ {
		quit <- true
	}
	wg.Wait()
}

// TestPutDiscard tests that entries put into a transaction and then discarded
// are not persisted.
func TestPutDiscard(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-put-discard.db"

	destroy(path)

	db, err := open(path)
	defer db.Close()
	assert.NoError(t, err)

	q, err := db.Queue("test")
	assert.NoError(t, err)

	// Put an entry into a transaction, but discard it
	tx := q.Transaction()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Close()

	// Read an entry from queue and ensure nothing is received
	rx := q.Transaction()
	v, err := rx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	rx.Close()
}

func TestTakeDiscard(t *testing.T, open OpenDB, destroy DestroyDB) {
	var err error

	path := "test-take-discard.db"
	destroy(path)

	db, err := open(path)
	defer db.Close()
	assert.NoError(t, err)

	q, err := db.Queue("test")
	assert.NoError(t, err)

	// Put an entry into a transaction
	tx := q.Transaction()
	defer tx.Close()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Commit()

	var rx *leviq.Txn
	var v []byte

	rx = q.Transaction()
	v, err = rx.Take()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Close()

	rx = q.Transaction()
	rx.Close()
	v, err = rx.Take()
	assert.NoError(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Commit()

	rx = q.Transaction()
	defer rx.Close()
	v, err = rx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
}

// TestNamespaces tests that items in disparate namespaces are kept apart.
func TestNamespaces(t *testing.T, open OpenDB, destroy DestroyDB) {
	path := "test-namespaces.db"
	destroy(path)

	db, err := open(path)
	defer db.Close()
	assert.NoError(t, err)

	qa, err := db.Queue("testa")
	assert.NoError(t, err)
	qb, err := db.Queue("testb")
	assert.NoError(t, err)

	// Write data to queue A
	tx := qa.Transaction()
	tx.Put([]byte("hello"))
	assert.NoError(t, tx.Commit())

	// Attempt to read from queue B
	tx = qb.Transaction()
	v, err := tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	assert.NoError(t, tx.Close())

	// Write data to queue B
	tx = qb.Transaction()
	tx.Put([]byte("world"))
	assert.NoError(t, tx.Commit())

	// Read from queue A
	tx = qa.Transaction()
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(v))
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	assert.NoError(t, tx.Close())

	// Read from queue B
	tx = qb.Transaction()
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Equal(t, "world", string(v))
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	assert.NoError(t, tx.Close())
}

func BenchmarkPuts1(b *testing.B, open OpenDB, destroy DestroyDB) {
	benchmarkPuts(b, open, destroy, 1)
}

func BenchmarkPuts10(b *testing.B, open OpenDB, destroy DestroyDB) {
	benchmarkPuts(b, open, destroy, 10)
}

func BenchmarkPuts100(b *testing.B, open OpenDB, destroy DestroyDB) {
	benchmarkPuts(b, open, destroy, 100)
}

func BenchmarkPuts1000(b *testing.B, open OpenDB, destroy DestroyDB) {
	benchmarkPuts(b, open, destroy, 1000)
}

func benchmarkPuts(b *testing.B, open OpenDB, destroy DestroyDB, n int) {
	path := "benchmark-puts.db"
	destroy(path)
	db, err := open(path)
	assert.Nil(b, err)
	defer db.Close()

	q, err := db.Queue("test")
	assert.Nil(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := q.Transaction()
		defer tx.Close()
		for j := 0; j < n; j++ {
			s := strconv.Itoa(i * j)
			tx.Put([]byte(s))
			i++
		}
		tx.Commit()
	}
	b.StopTimer()
}

// BenchmarkTake benchmarks the speed at which items can be taken.
func BenchmarkTake(b *testing.B, open OpenDB, destroy DestroyDB) {
	path := "benchmark-take.db"
	destroy(path)
	db, err := open(path)
	assert.Nil(b, err)
	defer db.Close()
	q, err := db.Queue("test")
	assert.Nil(b, err)
	benchmarkTake(b, open, destroy, q)
}

func benchmarkTake(b *testing.B, open OpenDB, destroy DestroyDB, q *leviq.Queue) {
	// Seed DB with items to take
	tx := q.Transaction()
	defer tx.Close()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		if err := tx.Put([]byte(s)); err != nil {
			b.Fatal("error during put:", err)
		}
	}
	tx.Commit()

	// Benchmark removal
	b.ResetTimer()
	rx := q.Transaction()
	defer rx.Close()
	for {
		v, err := rx.Take()
		if err != nil {
			b.Fatal("error during take:", err)
		}
		if len(v) == 0 {
			break
		}
	}
	rx.Commit()
	b.StopTimer()
}
