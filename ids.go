package levelpipe

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/sdming/gosnow"
)

var snow *gosnow.SnowFlake

func init() {
	var err error
	snow, err = gosnow.Default()
	if err != nil {
		log.Fatalln(err)
	}
}

type ID uint64

const (
	NilID ID = 0
)

// NewID generates an ID based on the current time.
func NewID() ID {
	id, err := snow.Next()
	if err != nil {
		panic(err)
	}
	return ID(id)
}

// KeyToID converts a key to an ID.
func KeyToID(k []byte) (ID, error) {
	id, n := binary.Uvarint(k)
	if n <= 0 {
		return NilID, fmt.Errorf("couldn't parse key: " + string(k))
	}
	return ID(id), nil
}

// Key returns the byte representation of this ID.
func (id ID) Key() []byte {
	k := make([]byte, 16)
	if binary.PutUvarint(k, uint64(id)) <= 0 {
		panic("couldn't write key")
	}
	return k
}

// IDHeap is a sorted set of IDs.
type IDHeap []ID

func (h IDHeap) Len() int            { return len(h) }
func (h IDHeap) Less(i, j int) bool  { return h[i] < h[j] }
func (h IDHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *IDHeap) Push(x interface{}) { *h = append(*h, x.(ID)) }
func (h *IDHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PopID pops the first ID from the heap.
func (h *IDHeap) PopID() ID {
	if len(*h) == 0 {
		return NilID
	}
	id := heap.Pop(h)
	return id.(ID)
}

// PushID pushes an ID onto the heap.
func (h *IDHeap) PushID(id ID) {
	heap.Push(h, id)
}

// NewIDHeap constructs a new ID heap.
func NewIDHeap() *IDHeap {
	h := &IDHeap{}
	heap.Init(h)
	return h
}
