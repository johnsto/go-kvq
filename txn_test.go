package kvq

import (
	"sync"
	"testing"
	"time"

	"github.com/johnsto/kvq/backend"
	"github.com/johnsto/kvq/internal"
	"github.com/stretchr/testify/assert"
)

type MockBucket struct {
	mutex sync.Mutex
	data  map[string][]byte
}

func NewMockBucket() *MockBucket {
	return &MockBucket{
		data: map[string][]byte{},
	}
}

func (b *MockBucket) ForEach(fn func(k, v []byte) error) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	for k, v := range b.data {
		if err := fn([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (b *MockBucket) Batch(fn func(backend.Batch) error) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	batch := NewMockBatch()
	if err := fn(batch); err != nil {
		return err
	}
	for k, v := range batch.puts {
		b.data[k] = v
	}
	for k, _ := range batch.deletes {
		delete(b.data, k)
	}
	return nil
}

func (b *MockBucket) Get(k []byte) ([]byte, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.data[string(k)], nil
}

func (b *MockBucket) Clear() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.data = map[string][]byte{}
	return nil
}

type MockBatch struct {
	puts    map[string][]byte
	deletes map[string]bool
}

func NewMockBatch() *MockBatch {
	return &MockBatch{
		puts:    map[string][]byte{},
		deletes: map[string]bool{},
	}
}

func (m *MockBatch) Put(k, v []byte) error {
	m.puts[string(k)] = v
	return nil
}

func (m *MockBatch) Delete(k []byte) error {
	m.deletes[string(k)] = true
	return nil
}

func (m *MockBatch) Close() {
	m.puts = map[string][]byte{}
	m.deletes = map[string]bool{}
}

func Test_Queue_Internals(t *testing.T) {
	bucket := NewMockBucket()
	queue := &Queue{
		bucket: bucket,
		mutex:  &sync.Mutex{},
		ids:    internal.NewIDHeap(),
		c:      make(chan struct{}, 3),
	}

	// Test initial (empty) state
	assert.Equal(t, 0, queue.Size(), "queue should be empty")
	assert.Empty(t, queue.getKeys(1),
		"queue should not immediately return any keys")
	assert.Empty(t, queue.awaitKeys(1, 0),
		"queue should not eventually return any keys")
	assert.Empty(t, queue.awaitKeys(1, 50*time.Millisecond),
		"queue should not eventually return any keys")

	// Clear and check still empty
	assert.NoError(t, queue.Clear())
	assert.Equal(t, 0, queue.Size(), "queue should be empty after clear")
	assert.Empty(t, queue.getKeys(1),
		"queue should not immediately return any keys after clear")
	assert.Empty(t, queue.awaitKeys(1, 50*time.Millisecond),
		"queue should not eventually return any keys after clear")

	// Put an ID on the queue, check it becomes available
	n, err := queue.putKey(internal.ID(1))
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.Size(), "queue should be of size 1")
	assert.Len(t, queue.getKeys(1), 1,
		"queue should immediately return 1 of requested 1 key")
	n, err = queue.putKey(internal.ID(1))
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Len(t, queue.awaitKeys(1, 50*time.Millisecond), 1,
		"queue should not eventually return 1 of requested 1 key")

	// Take more keys than actually available
	n, err = queue.putKey(internal.ID(1))
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.Size(), "queue should be of size 1")
	assert.Len(t, queue.getKeys(2), 1,
		"queue should immediately return 1 of requested 2 keys")
	n, err = queue.putKey(internal.ID(1))
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Len(t, queue.awaitKeys(2, 50*time.Millisecond), 1,
		"queue should not eventually return 1 of requested 2 keys")

	// Put more keys than there is room available for
	n, err = queue.putKey(internal.ID(1))
	assert.Equal(t, 1, n)
	assert.NoError(t, err)
	assert.Equal(t, 1, queue.Size(), "queue should contain 1 key")
	n, err = queue.putKey(internal.ID(2), internal.ID(3))
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	assert.Equal(t, 3, queue.Size(), "queue should contain 3 keys")
	n, err = queue.putKey(internal.ID(2), internal.ID(3))
	assert.Equal(t, 0, n, "4th key should be rejected")
	assert.Equal(t, err, ErrInsufficientCapacity,
		"4th key should return capacity error")
	assert.Equal(t, 3, queue.Size(), "queue should still contain 3 keys")
	assert.Len(t, queue.getKeys(4), 3,
		"queue should immediately return 3 of requested 4 keys")

	// Enact a change to underlying bucket
	kv1 := kv{[]byte("k1"), []byte("v1")}
	kv2 := kv{[]byte("k2"), []byte("v2")}
	kv3 := kv{[]byte("k3"), []byte("v3")}
	assert.NoError(t, queue.enact([]kv{kv1, kv2, kv3}, nil),
		"queue should enact puts s without error")
	assert.EqualValues(t, "v1", bucket.data["k1"], "bucket should contain put kv1")
	assert.EqualValues(t, "v2", bucket.data["k2"], "bucket should contain put kv2")
	assert.EqualValues(t, "v3", bucket.data["k3"], "bucket should contain put kv3")
	assert.NoError(t, queue.enact(nil, []kv{kv1, kv2, kv3}),
		"queue should enact takes without error")
	assert.Nil(t, bucket.data["k1"], "bucket should no longer contain kv1")
	assert.Nil(t, bucket.data["k2"], "bucket should no longer contain kv2")
	assert.Nil(t, bucket.data["k3"], "bucket should no longer contain kv3")

	// Take keys
	kv1 = kv{internal.ID(1).Key(), []byte("v1")}
	kv2 = kv{internal.ID(2).Key(), []byte("v2")}
	kv3 = kv{internal.ID(3).Key(), []byte("v3")}
	assert.NoError(t, queue.enact([]kv{kv1, kv2}, nil),
		"queue should enact puts without error")
	n, err = queue.putKey(internal.ID(1), internal.ID(2), internal.ID(3))
	assert.Equal(t, 3, n, "3 keys should be accepted")
	assert.NoError(t, err)
	n, err = queue.putKey(internal.ID(4))
	assert.Equal(t, 0, n, "4th key should be rejected")
	ids, keys, values, err := queue.take(2, 0)
	assert.NoError(t, err, "take should not error")
	assert.Equal(t, []internal.ID{internal.ID(1), internal.ID(2)}, ids)
	assert.Equal(t, [][]byte{internal.ID(1).Key(), internal.ID(2).Key()}, keys)
	assert.Equal(t, [][]byte{kv1.v, kv2.v}, values)
}

func Test_Queue_Transaction(t *testing.T) {
	bucket := NewMockBucket()
	queue := &Queue{
		bucket: bucket,
		mutex:  &sync.Mutex{},
		ids:    internal.NewIDHeap(),
		c:      make(chan struct{}, 3),
	}

	// Create and close txn
	txn := queue.Transaction()
	assert.NoError(t, txn.Close(), "txn should close without error")

	// Create, put and close txn
	txn = queue.Transaction()
	assert.NoError(t, txn.Put([]byte("v1")), "txn put should not error")
	assert.Equal(t, 0, queue.ids.Len(), "queue should remain empty before commit")
	assert.Equal(t, 1, txn.puts.Len(), "txn should contain 1 put ID")
	assert.Len(t, txn.putValues, 1, "txn should contain 1 put value")
	assert.NoError(t, txn.Close(), "txn should close without error")
	assert.Equal(t, 0, txn.puts.Len(), "txn should be empty after close")
	assert.Len(t, txn.putValues, 0, "txn should contain 1 put value")

	// Create, put, take and close txn
	txn = queue.Transaction()
	assert.NoError(t, txn.Put([]byte("v1")), "txn put should not error")
	assert.Equal(t, 0, queue.ids.Len(), "queue should remain empty before commit")
	v, err := txn.Take()
	assert.NoError(t, err, "txn take should not error")
	assert.Nil(t, v, "put value should not be taken from txn")
	assert.NoError(t, txn.Close(), "txn should close without error")

	// Create and commit txn
	txn = queue.Transaction()
	assert.NoError(t, txn.Put([]byte("v1")), "txn put should not error")
	assert.Equal(t, 0, queue.ids.Len(),
		"queue should remain empty before commit")
	v, err = txn.Take()
	assert.NoError(t, err, "txn take should not error")
	assert.Nil(t, v, "put value should not be taken from txn")
	assert.NoError(t, txn.Commit(), "txn should commit without error")
	assert.Equal(t, 1, queue.ids.Len(),
		"queue should contain single item after commit")
	assert.NoError(t, txn.Commit(), "empty txn should commit without error")
	assert.Equal(t, 1, queue.ids.Len(),
		"queue should still contain single item after empty commit")
	v, err = txn.Take()
	assert.NoError(t, err, "txn take should not error")
	assert.Equal(t, []byte("v1"), v, "taken value should match put value")
	assert.NoError(t, txn.Commit(), "txn should commit without error")
	assert.Equal(t, 0, queue.ids.Len(), "queue should be empty after take")

	// Create and take exact number of available items
	txn = queue.Transaction()
	assert.NoError(t, txn.Put([]byte("v1")), "txn put should not error")
	assert.NoError(t, txn.Put([]byte("v2")), "txn put should not error")
	assert.NoError(t, txn.Commit(), "commit without error")
	vs, err := txn.TakeN(2, time.Minute)
	assert.NoError(t, err, "take 2 within 1 minute should be without error")
	assert.EqualValues(t, vs, [][]byte{[]byte("v1"), []byte("v2")})

	// Create and commit excessive txn
	txn = queue.Transaction()
	assert.NoError(t, txn.Put([]byte("v1")), "txn put should not error")
	assert.NoError(t, txn.Put([]byte("v2")), "txn put should not error")
	assert.NoError(t, txn.Put([]byte("v3")), "txn put should not error")
	assert.NoError(t, txn.Put([]byte("v4")), "txn put should not error")
	assert.EqualError(t, txn.Commit(), "insufficient queue capacity",
		"txn put should fail with insufficient capacity")
}
