package goleveldb

import (
	"github.com/johnsto/leviq"
	"github.com/johnsto/leviq/backend/test"
	"testing"
)

func OpenDB(path string) (*leviq.DB, error) {
	db, err := Open(path)
	if err != nil {
		return nil, err
	}
	return leviq.NewDB(db), nil
}

func TestInit(t *testing.T) {
	test.TestInit(t, OpenDB, Destroy)
}

func TestQueueSingle(t *testing.T) {
	test.TestQueueSingle(t, OpenDB, Destroy)
}

func TestQueueMulti(t *testing.T) {
	test.TestQueueMulti(t, OpenDB, Destroy)
}

func TestQueueOrdered(t *testing.T) {
	test.TestQueueOrdered(t, OpenDB, Destroy)
}

func TestQueueThreaded(t *testing.T) {
	test.TestQueueThreaded(t, OpenDB, Destroy)
}

func TestPutDiscard(t *testing.T) {
	test.TestPutDiscard(t, OpenDB, Destroy)
}

func TestTakeDiscard(t *testing.T) {
	test.TestTakeDiscard(t, OpenDB, Destroy)
}

func TestNamespaces(t *testing.T) {
	test.TestNamespaces(t, OpenDB, Destroy)
}

func BenchmarkPuts1(b *testing.B) {
	test.BenchmarkPuts1(b, OpenDB, Destroy)
}

func BenchmarkPuts10(b *testing.B) {
	test.BenchmarkPuts10(b, OpenDB, Destroy)
}

func BenchmarkPuts100(b *testing.B) {
	test.BenchmarkPuts100(b, OpenDB, Destroy)
}

func BenchmarkPuts1000(b *testing.B) {
	test.BenchmarkPuts1000(b, OpenDB, Destroy)
}

func BenchmarkTake(b *testing.B) {
	test.BenchmarkTake(b, OpenDB, Destroy)
}
