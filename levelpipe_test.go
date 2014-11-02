package levelpipe

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPipeSingle(t *testing.T) {
	err := DestroyPipe("pipe.db")
	p, err := NewPipe("pipe.db")
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

	tx := p.Put()
	for i := 0; i < 100; i++ {
		s := strconv.Itoa(i)
		tx.Put([]byte(s))
		keys[s] = true
	}
	tx.Commit()

	rx := p.Take()
	for len(keys) != 0 {
		v, err := rx.Take()
		assert.NoError(t, err)
		s := string(v)
		assert.True(t, keys[s])
		delete(keys, s)
	}
	rx.Commit()

	// Verify there are no more items in the queue
	rx = p.Take()
	v, err := rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
}

func TestPipeMulti(t *testing.T) {
	err := DestroyPipe("pipe.db")
	p, err := NewPipe("pipe.db")
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		p.Close()
	}()
	if err := p.Clear(); err != nil {
		log.Fatalln(err)
	}

	tx := p.Put()
	defer tx.Close()
	tx.Put([]byte("a"))
	tx.Put([]byte("b"))
	tx.Put([]byte("c"))
	tx.Commit()

	rxA := p.Take()
	defer rxA.Close()
	vA, errA := rxA.Take()
	assert.Nil(t, errA)
	assert.NotNil(t, vA)
	rxB := p.Take()
	defer rxB.Close()
	vB, errB := rxB.Take()
	assert.Nil(t, errB)
	assert.NotNil(t, vB)
	rxC := p.Take()
	defer rxC.Close()
	vC, errC := rxC.Take()
	assert.Nil(t, errC)
	assert.NotNil(t, vC)

	assert.Condition(t, func() bool {
		return !bytes.Equal(vA, vB) && !bytes.Equal(vB, vC)
	}, "each taken value should be different")

}

func TestPipeThreaded(t *testing.T) {
	err := DestroyPipe("pipe.db")
	p, err := NewPipe("pipe.db")
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
		// Fill pipe with items from input channel
		wg.Add(1)
		go func() {
			m := 0
			for s := range inp {
				tx := p.Put()
				tx.Put([]byte(s))
				tx.Commit()
				tx.Close()
				m++
			}
			wg.Done()
		}()

		// Pull items from pipe and put into output channel
		wg.Add(1)
		go func() {
			m := 0
			for active {
				rx := p.Take()
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
	DestroyPipe("test.db")

	pipe, err := NewPipe("test.db")
	defer pipe.Close()
	assert.Nil(t, err)

	// Put an entry into a transaction, but discard it
	tx := pipe.Put()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Discard()

	// Read an entry from pipe and ensure nothing is received
	rx := pipe.Take()
	v, err := rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
	rx.Discard()
}

func TestTakeDiscard(t *testing.T) {
	var err error

	DestroyPipe("test.db")

	pipe, err := NewPipe("test.db")
	defer pipe.Close()
	assert.Nil(t, err)

	// Put an entry into a transaction
	tx := pipe.Put()
	assert.Nil(t, tx.Put([]byte("test")))
	tx.Commit()
	tx.Close()

	var rx *Take
	var v []byte

	rx = pipe.Take()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Discard()
	rx.Close()

	rx = pipe.Take()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), v)
	rx.Commit()
	rx.Close()

	rx = pipe.Take()
	v, err = rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
	rx.Close()
}

func BenchmarkWrite(b *testing.B) {
	DestroyPipe("benchmark.db")
	p, err := NewPipe("benchmark.db")
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
	benchmarkWrite(b, p)
}

func BenchmarkWriteSync(b *testing.B) {
	DestroyPipe("benchmark.db")
	p, err := NewPipe("benchmark.db")
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
	p.SetSync(true)
	benchmarkWrite(b, p)
}

func benchmarkWrite(b *testing.B, p *Pipe) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := p.Put()
		s := strconv.Itoa(i)
		tx.Put([]byte(s))
		tx.Commit()
	}
	b.StopTimer()
}

func BenchmarkRead(b *testing.B) {
	DestroyPipe("benchmark.db")
	p, err := NewPipe("benchmark.db")
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
	benchmarkRead(b, p)
}

func benchmarkRead(b *testing.B, p *Pipe) {
	tx := p.Put()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		if err := tx.Put([]byte(s)); err != nil {
			b.Fatal("error during put:", err)
		}
	}
	tx.Commit()
	b.ResetTimer()
	rx := p.Take()
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
