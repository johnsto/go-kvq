/*

Package leviq provides a transactional, disk-backed queue built on top of
LevelDB, using levigo [1].

leviq.Open opens and creates queues.

	q, err := leviq.Open("queue.db", nil)

(Remember to Close() your queue when finished with it.)

To push data onto a queue, start a transaction, add values, and then commit.

	t, err := q.Transaction()
	defer t.Close()
	t.Put([]byte("hello"))
	t.Put([]byte("world"))
	err = t.Commit()

To take data from the queue, start a transaction, take values, and the commit.

	t, err := q.Transaction()
	defer t.Close()
	value, err := t.Take()
	err = t.Commit()

Items on the queue are keyed by the time at which they were Put(), so the push
order should generally match the pop order (barring any run-time pauses).

Data in transactions are only persisted when Commit() is called. Any items
put in a transaction do not become available for taking until the transaction
is committed. Similarly, any items taken in a transaction are made available
again if the transaction is closed without being committed.

By default, LevelDB does not flush data to disk before returning - so data
can be lost even after Commit() has been called. To decrease the chances of
data loss, enable sync:

	q, err := leviq.Open("queue.db", nil)
	q.SetSync(true)

Note that enabling sync drastically decreases write performance.

For an indication of read/write performance, try running the included
benchmarks (note the disparity between Sync and non-Sync write performance).

	GOMAXPROCS=4 go test -bench=.

The benchmarks are set up such that each Put or Take constitutes a single 'op'
in Go's benchmarking parlance, therefore making it easy to convert to a very
rough events/sec measurement.

[1] https://github.com/jmhodges/levigo

*/
package leviq
