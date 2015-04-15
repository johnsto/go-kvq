/*

Package kvq is a persistent transactional queue. It supports goleveldb, levigo and Bolt as backends.

To open a queue, first get a database instance, and then the queue itself. A DB can contain many queues of different names.

	db, err := kvq.Open("db.db")
	queue, err := db.Queue("tweets")

(Remember to Close() when finished)

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

To retrieve a certain number of items within a timeframe, use Txn.TakeN:

	values, err := t.TakeN(100, 5 * time.Second)

This will take up to 100 items within 5 seconds (total), and return the values
when either of those conditions are met.

Data in transactions are only persisted when Commit() is called. Any items
put in a transaction do not become available for taking until the transaction
is committed. Similarly, any items taken in a transaction are made available
again if the transaction is closed without being committed.

*/
package kvq // import "github.com/johnsto/kvq"
