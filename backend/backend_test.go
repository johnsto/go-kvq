package backend_test

import (
	"testing"

	. "github.com/johnsto/kvq/backend"
	"github.com/johnsto/kvq/backend/bolt"
	"github.com/johnsto/kvq/backend/goleveldb"
	"github.com/johnsto/kvq/backend/levigo"
	"github.com/stretchr/testify/assert"
)

func TestGoLevelDB(t *testing.T) {
	goleveldb.Destroy("test.db")
	db, err := goleveldb.Open("test.db")
	assert.NoError(t, err, "opening goleveldb should not error")
	testBucket(t, db)
}

func TestLevigo(t *testing.T) {
	levigo.Destroy("test.db")
	db, err := levigo.Open("test.db")
	assert.NoError(t, err, "opening levigo should not error")
	testBucket(t, db)
}

func TestBolt(t *testing.T) {
	bolt.Destroy("test.db")
	db, err := bolt.Open("test.db")
	assert.NoError(t, err, "opening bolt should not error")
	testBucket(t, db)
}

func testBucket(t *testing.T, db DB) {
	bucket, err := db.Bucket("test")
	assert.NoError(t, err, "getting test bucket should not error")
	assert.NotNil(t, bucket, "test bucket should not be nil")

	// Empty bucket tests
	bucket.ForEach(func(k, v []byte) error {
		assert.Fail(t, "bucket should be empty", "contains key '%s'", string(k))
		return nil
	})
	assert.NoError(t, bucket.Clear(), "clearing empty bucket should not error")

	assert.NoError(t, bucket.Batch(func(b Batch) error {
		return nil
	}), "empty batch should not error")

	assert.NoError(t, bucket.Batch(func(b Batch) error {
		return b.Delete([]byte("k1"))
	}), "deleting non-existant key in batch should not error")

	// Tests on single-item bucket
	assert.NoError(t, bucket.Batch(func(b Batch) error {
		return b.Put([]byte("k1"), []byte("v1"))
	}), "putting key in batch should not error")

	v1, err := bucket.Get([]byte("k1"))
	assert.NoError(t, err, "getting a value after put should not error")
	assert.Equal(t, []byte("v1"), v1, "get value should match put value")

	iterations := 0
	bucket.ForEach(func(k, v []byte) error {
		assert.Equal(t, []byte("k1"), k, "should iterate through k1")
		assert.Equal(t, []byte("v1"), v, "should iterate through v1")
		iterations++
		return nil
	})
	assert.Equal(t, 1, iterations, "should iterate only once")

	assert.NoError(t, bucket.Batch(func(b Batch) error {
		return b.Delete([]byte("k1"))
	}), "deleting  key in batch should not error")

	v1, err = bucket.Get([]byte("k1"))
	assert.Equal(t, ErrKeyNotFound, err, "getting a non-existent key should fail")
	assert.Nil(t, v1, "get value should be nil")

	// Tests on multi-item bucket
	assert.NoError(t, bucket.Batch(func(b Batch) error {
		if err := b.Put([]byte("k2"), []byte("v2")); err != nil {
			return err
		}
		return b.Put([]byte("k3"), []byte("v3"))
	}), "putting key in batch should not error")

	v2, err := bucket.Get([]byte("k2"))
	assert.NoError(t, err, "getting a value after put should not error")
	assert.Equal(t, []byte("v2"), v2, "get value should match put value")

	v3, err := bucket.Get([]byte("k3"))
	assert.NoError(t, err, "getting a value after put should not error")
	assert.Equal(t, []byte("v3"), v3, "get value should match put value")

	iterations = 0
	bucket.ForEach(func(k, v []byte) error {
		if iterations == 0 {
			assert.Equal(t, []byte("k2"), k, "should iterate through k2")
			assert.Equal(t, []byte("v2"), v, "should iterate through v2")
		} else if iterations == 1 {
			assert.Equal(t, []byte("k3"), k, "should iterate through k3")
			assert.Equal(t, []byte("v3"), v, "should iterate through v3")
		}
		iterations++
		return nil
	})
	assert.Equal(t, 2, iterations, "should iterate twice")

	assert.NoError(t, bucket.Batch(func(b Batch) error {
		return b.Delete([]byte("k9"))
	}), "deleting key not in batch should not error")

	assert.NoError(t, bucket.Batch(func(b Batch) error {
		return b.Delete([]byte("k2"))
	}), "deleting key in batch should not error")

	v1, err = bucket.Get([]byte("k1"))
	assert.Equal(t, ErrKeyNotFound, err, "getting a non-existent key should fail")
	assert.Nil(t, v1, "get value should be nil")

	v3, err = bucket.Get([]byte("k3"))
	assert.NoError(t, err, "getting a key should not fail")
	assert.Equal(t, []byte("v3"), v3, "the got value should match")

}
