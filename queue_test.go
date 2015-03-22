package leviq

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestInit ensures that data are loaded again correctly from disk.
func TestInit(t *testing.T) {
	err := Destroy("queue.db")

	// Open initial DB
	db, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	q, err := db.Queue("test")
	assert.Nil(t, err)
	q.SetSync(true)

	table := []string{"a", "b", "c"}

	// Put initial values
	tx := q.Transaction()
	for _, value := range table {
		tx.Put([]byte(value))
	}
	tx.Commit()
	tx.Close()

	// Re-open DB
	db.Close()
	db, err = Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}

	q, err = db.Queue("test")
	assert.Nil(t, err)
	q.SetSync(true)

	// Read expected values
	tx = q.Transaction()
	for _, value := range table {
		v, err := tx.Take()
		assert.NoError(t, err)
		assert.Equal(t, []byte(value), v)
	}
	tx.Commit()

	// Re-open DB
	db.Close()
	db, err = Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}

	q, err = db.Queue("test")
	assert.Nil(t, err)
	q.SetSync(true)

	// Ensure read values are now gone
	tx = q.Transaction()
	v, err := tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	tx.Close()
	db.Close()
}

// TestQueueSingle tests a batch of puts and takes in a single transaction
func TestQueueSingle(t *testing.T) {
	err := Destroy("queue.db")
	db, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.Nil(t, err)
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
	assert.Nil(t, err)
	assert.Nil(t, v)
}

// TestQueueMulti tests a series of puts/takes in a number of transactions
func TestQueueMulti(t *testing.T) {
	err := Destroy("queue.db")
	db, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.Nil(t, err)
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
		assert.Nil(t, err)
		assert.Equal(t, exp, string(act))
		rx.Commit()
	}
}

// TestQueueOrdered tests that items come out of the queue in the correct
// order.
func TestQueueOrdered(t *testing.T) {
	err := Destroy("queue.db")
	db, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.Nil(t, err)
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
		assert.Nil(t, err)
		assert.Equal(t, []byte{i}, act)
	}
	rx.Commit()
}

// TestQueueThreaded puts and takes items from a number of simultaneous
// goroutines.
func TestQueueThreaded(t *testing.T) {
	err := Destroy("queue.db")
	db, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	q, err := db.Queue("test")
	assert.Nil(t, err)
	if err := q.Clear(); err != nil {
		log.Fatalln(err)
	}

	routines := 4
	n := 1000

	inp := make(chan string)
	outp := make(chan string)
	active := true
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
			for active {
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

	active = false
	wg.Wait()
}

// TestPutDiscard tests that entries put into a transaction and then discarded
// are not persisted.
func TestPutDiscard(t *testing.T) {
	Destroy("test.db")

	db, err := Open("test.db", nil)
	defer db.Close()
	assert.Nil(t, err)

	q, err := db.Queue("test")
	assert.Nil(t, err)

	// Put an entry into a transaction, but discard it
	tx := q.Transaction()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Close()

	// Read an entry from queue and ensure nothing is received
	rx := q.Transaction()
	v, err := rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
	rx.Close()
}

func TestTakeDiscard(t *testing.T) {
	var err error

	Destroy("test.db")

	db, err := Open("test.db", nil)
	defer db.Close()
	assert.Nil(t, err)

	q, err := db.Queue("test")
	assert.Nil(t, err)

	// Put an entry into a transaction
	tx := q.Transaction()
	defer tx.Close()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Commit()

	var rx *Txn
	var v []byte

	rx = q.Transaction()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Close()

	rx = q.Transaction()
	rx.Close()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Commit()

	rx = q.Transaction()
	defer rx.Close()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
}

// TestNamespaces tests that items in disparate namespaces are kept apart.
func TestNamespaces(t *testing.T) {
	Destroy("test.db")

	db, err := Open("test.db", nil)
	defer db.Close()
	assert.Nil(t, err)

	qa, err := db.Queue("testa")
	assert.Nil(t, err)
	qa.SetSync(true)
	qb, err := db.Queue("testb")
	assert.Nil(t, err)
	qb.SetSync(true)

	// Write data to queue A
	tx := qa.Transaction()
	tx.Put([]byte("hello"))
	tx.Commit()

	// Attempt to read from queue B
	tx = qb.Transaction()
	v, err := tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	tx.Close()

	// Write data to queue B
	tx = qb.Transaction()
	tx.Put([]byte("world"))
	tx.Commit()

	// Read from queue A
	tx = qa.Transaction()
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Equal(t, "hello", string(v))
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	tx.Close()

	// Read from queue B
	tx = qb.Transaction()
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Equal(t, "world", string(v))
	v, err = tx.Take()
	assert.NoError(t, err)
	assert.Nil(t, v)
	tx.Close()
}

func BenchmarkPuts1(b *testing.B) {
	benchmarkPuts(b, 1, false)
}

func BenchmarkPuts10(b *testing.B) {
	benchmarkPuts(b, 10, false)
}

func BenchmarkPuts100(b *testing.B) {
	benchmarkPuts(b, 100, false)
}

func BenchmarkPuts1000(b *testing.B) {
	benchmarkPuts(b, 1000, false)
}

func BenchmarkPutsSync1(b *testing.B) {
	benchmarkPuts(b, 1, true)
}

func BenchmarkPutsSync10(b *testing.B) {
	benchmarkPuts(b, 10, true)
}

func BenchmarkPutsSync100(b *testing.B) {
	benchmarkPuts(b, 100, true)
}

func BenchmarkPutsSync1000(b *testing.B) {
	benchmarkPuts(b, 1000, true)
}

func benchmarkPuts(b *testing.B, n int, sync bool) {
	Destroy("benchmark.db")
	db, err := Open("benchmark.db", nil)
	assert.Nil(b, err)
	defer db.Close()

	q, err := db.Queue("test")
	assert.Nil(b, err)
	q.SetSync(sync)

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
func BenchmarkTake(b *testing.B) {
	Destroy("benchmark.db")
	db, err := Open("benchmark.db", nil)
	assert.Nil(b, err)
	defer db.Close()
	q, err := db.Queue("test")
	assert.Nil(b, err)
	benchmarkTake(b, q)
}

func benchmarkTake(b *testing.B, q *Queue) {
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
