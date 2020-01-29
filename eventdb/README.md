# EventDB - Event store

Package eventdb (event sourcing) is a small library for event sourcing

Event sourcing persists the state of a business entity such an Order or a
Customer as a sequence of state-changing events. Whenever the state of a
business entity changes, a new event is appended to the list of events.
Since saving an event is a single operation, it is inherently atomic.
The application reconstructs an entity’s current state by replaying the events.

Applications persist events in an event store, which is a database of events.
The store has an API for adding and retrieving an entity’s events.
The event store also behaves like a message broker. It provides an API that
enables services to subscribe to events. When a service saves an event in
the event store, it is delivered to all interested subscribers.

Some entities, such as a Customer, can have a large number of events.
In order to optimise loading, an application can periodically save a snapshot
of an entity’s current state. To reconstruct the current state,
the application finds the most recent snapshot and the events that have
occurred since that snapshot. As a result, there are fewer events to replay.

**There is no Delete**

It is not possible to jump into the time machine and say that an event never
occurred (eg: delete a previous event).
As such, it is necessary to model a delete explicitly as a new transaction.
e.g. InvoiceCreated, InvoiceUpdated, and InvoiceDeleted


## Example

```go
func main() {
	ctx := context.TODO()

	// Create KV store
	os.Mkdir("tmpdb", 0770)
	kvs, err := bbolt.Open(path.Join("./tmpdb", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}

	// Create or open "bucket"
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		panic(err)
	}

	// Initialise event store layer
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		panic(err)
	}

	// Just Create a stream with ID 123
	_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		return tx.CreateStream("123")
	})
	if err != nil {
		panic(err)
	}
}
````