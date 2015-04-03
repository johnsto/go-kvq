package goleveldb

import "github.com/johnsto/leviq"
import "github.com/johnsto/leviq/tests"
import "testing"

func OpenDB(path string) (*leviq.DB, error) {
	db, err := NewMemDB() //Open(path)
	if err != nil {
		return nil, err
	}
	return leviq.NewDB(db), nil
}

func TestInit(t *testing.T) {
	tests.TestInit(t, OpenDB, Destroy)
}

func TestQueueSingle(t *testing.T) {
	tests.TestQueueSingle(t, OpenDB, Destroy)
}

func TestQueueMulti(t *testing.T) {
	tests.TestQueueMulti(t, OpenDB, Destroy)
}

func TestQueueOrdered(t *testing.T) {
	tests.TestQueueOrdered(t, OpenDB, Destroy)
}

func TestQueueThreaded(t *testing.T) {
	tests.TestQueueThreaded(t, OpenDB, Destroy)
}

func TestPutDiscard(t *testing.T) {
	tests.TestPutDiscard(t, OpenDB, Destroy)
}

func TestTakeDiscard(t *testing.T) {
	tests.TestTakeDiscard(t, OpenDB, Destroy)
}

func TestNamespaces(t *testing.T) {
	tests.TestNamespaces(t, OpenDB, Destroy)
}

func BenchmarkPuts1(b *testing.B) {
	tests.BenchmarkPuts1(b, OpenDB, Destroy)
}

func BenchmarkPuts10(b *testing.B) {
	tests.BenchmarkPuts10(b, OpenDB, Destroy)
}

func BenchmarkPuts100(b *testing.B) {
	tests.BenchmarkPuts100(b, OpenDB, Destroy)
}

func BenchmarkPuts1000(b *testing.B) {
	tests.BenchmarkPuts1000(b, OpenDB, Destroy)
}

func BenchmarkTake(b *testing.B) {
	tests.BenchmarkTake(b, OpenDB, Destroy)
}
