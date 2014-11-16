package leviq

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueSingle(t *testing.T) {
	err := Destroy("queue.db")
	p, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		p.Close()
	}()
	if err := p.Clear(); err != nil {
		log.Fatalln(err)
	}

	keys := make(map[string]bool)

	tx := p.Transaction()
	defer tx.Close()
	for i := 0; i < 100; i++ {
		s := strconv.Itoa(i)
		tx.Put([]byte(s))
		keys[s] = true
	}
	tx.Commit()

	rx := p.Transaction()
	defer rx.Close()
	for len(keys) != 0 {
		v, err := rx.Take()
		assert.NoError(t, err)
		s := string(v)
		assert.True(t, keys[s])
		delete(keys, s)
	}
	rx.Commit()

	// Verify there are no more items in the queue
	rx = p.Transaction()
	defer rx.Close()
	v, err := rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
}

func TestQueueMulti(t *testing.T) {
	err := Destroy("queue.db")
	p, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		p.Close()
	}()
	if err := p.Clear(); err != nil {
		log.Fatalln(err)
	}

	exps := [][]byte{[]byte("a"), []byte("b"), []byte("c")}

	tx := p.Transaction()
	defer tx.Close()
	for _, b := range exps {
		tx.Put(b)
	}
	tx.Commit()

	exps = append(exps, nil)
	for _, exp := range exps {
		rx := p.Transaction()
		defer rx.Close()
		act, err := rx.Take()
		assert.Nil(t, err)
		assert.Equal(t, exp, act)
	}
}

func TestQueueThreaded(t *testing.T) {
	err := Destroy("queue.db")
	p, err := Open("queue.db", nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		p.Close()
	}()
	if err := p.Clear(); err != nil {
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
				tx := p.Transaction()
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
				rx := p.Transaction()
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

	queue, err := Open("test.db", nil)
	defer queue.Close()
	assert.Nil(t, err)

	// Put an entry into a transaction, but discard it
	tx := queue.Transaction()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Close()

	// Read an entry from queue and ensure nothing is received
	rx := queue.Transaction()
	v, err := rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
	rx.Close()
}

func TestTakeDiscard(t *testing.T) {
	var err error

	Destroy("test.db")

	queue, err := Open("test.db", nil)
	defer queue.Close()
	assert.Nil(t, err)

	// Put an entry into a transaction
	tx := queue.Transaction()
	defer tx.Close()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Commit()

	var rx *Txn
	var v []byte

	rx = queue.Transaction()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Close()

	rx = queue.Transaction()
	rx.Close()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Commit()

	rx = queue.Transaction()
	defer rx.Close()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
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
	p, err := Open("benchmark.db", nil)
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
	p.SetSync(sync)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := p.Transaction()
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

func BenchmarkTake(b *testing.B) {
	Destroy("benchmark.db")
	p, err := Open("benchmark.db", nil)
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
	benchmarkTake(b, p)
}

func benchmarkTake(b *testing.B, p *Queue) {
	tx := p.Transaction()
	defer tx.Close()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		if err := tx.Put([]byte(s)); err != nil {
			b.Fatal("error during put:", err)
		}
	}
	tx.Commit()
	b.ResetTimer()
	rx := p.Transaction()
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
