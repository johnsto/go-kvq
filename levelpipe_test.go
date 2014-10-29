package main

import (
	"log"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipe(t *testing.T) {
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

	inDone := false
	inGroup := &sync.WaitGroup{}
	outGroup := &sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		inGroup.Add(1)
		go func() {
			tx := p.Put()
			for i := 0; i < 100; i++ {
				s := strconv.Itoa(i)
				err := tx.Put([]byte(s))
				assert.Nil(t, err)
			}
			tx.Commit()
			inGroup.Done()
		}()
	}

	for i := 0; i < 5; i++ {
		outGroup.Add(1)
		go func() {
			for {
				rx := p.Take()
				v, err := rx.Take()
				assert.Nil(t, err)
				if inDone && v == nil {
					break
				}
				rx.Commit()
			}
			outGroup.Done()
		}()
	}

	inGroup.Wait()
	inDone = true
	outGroup.Wait()

	// Verify there are no more items in the queue
	rx := p.Take()
	v, err := rx.Take()
	assert.Nil(t, err)
	assert.Nil(t, v)
}

func BenchmarkWrite(b *testing.B) {
	DestroyPipe("benchmark.db")
	p, err := NewPipe("benchmark.db")
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
	b.ResetTimer()
	tx := p.Put()
	for i := 0; i < b.N; i++ {
		s := strconv.Itoa(i)
		tx.Put([]byte(s))
	}
	tx.Commit()
	b.StopTimer()
}

func BenchmarkRead(b *testing.B) {
	DestroyPipe("benchmark.db")
	p, err := NewPipe("benchmark.db")
	assert.Nil(b, err)
	defer func() {
		p.Close()
	}()
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
