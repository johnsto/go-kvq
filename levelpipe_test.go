package main

import (
	"log"
	"testing"

	"github.com/coreos/etcd/third_party/github.com/stretchr/testify/assert"
)

func Test_Pipe(t *testing.T) {
	p, err := NewPipe("pipe.db")
	if err != nil {
		log.Fatalln(err)
	}

	tx := p.Put()
	tx.Put([]byte("e"))
	tx.Put([]byte("a"))
	tx.Put([]byte("f"))
	tx.Put([]byte("c"))
	tx.Put([]byte("b"))
	tx.Put([]byte("d"))
	tx.Commit()

	rx := p.Take()
	take := func() []byte {
		v, err := rx.Take()
		assert.NoError(t, err)
		return v
	}
	assert.Equal(t, []byte("a"), take())
	assert.Equal(t, []byte("b"), take())
	assert.Equal(t, []byte("c"), take())
	rx.Commit()
}
