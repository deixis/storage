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

## Subscription

### Volatile

SubscriptionTypeVolatile calls a given function for events written after
establishing the subscription. They are useful when you need notification
of new events with minimal latency, but where it's not necessary to process
every event.

For example, if a stream has 100 events in it when a subscriber connects,
the subscriber can expect to see event number 101 onwards until the time
the subscription is closed or dropped.

### Catch up

SubscriptionTypeCatchUp specifies a starting point, in the form of an event
number or transaction file position. You call the function for events from
the starting point until the end of the stream, and then for subsequently
written events.

For example, if you specify a starting point of 50 when a stream has 100
events, the subscriber can expect to see events 51 through 100, and then
any events are subsequently written until you drop or close the
subscription.

### Persistent

SubscriptionTypePersistent supports the "competing consumers" messaging
pattern and are useful when you need to distribute messages to many workers.

EventDB saves the subscription state and allows for at-least-once delivery
guarantees across multiple consumers on the same stream.
It is possible to have many groups of consumers compete on the same stream,
with each group getting an at-least-once guarantee.

Note: Persistent require to create a persistent subscription first.

## Projection

Projection is an EventDB subsystem that lets you write new events or link
existing events to streams in a reactive manner.

Projections are good at solving one specific query type, a category known
as 'temporal correlation queries'.
This query type is common in business systems and few can execute these
queries well.

TODO: This feature is still under development

## Examples

### Create a stream

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
```

### Subscribe to stream updates

```go
subscription, err := eventStore.Subscribe(
	context.TODO(),
	"my-stream", // Stream must exist
	"my-subscription-group", // Only deliver to a single group subscriber
	eventdb.SubscriptionTypeVolatile,
	func(ctx context.Context, e eventdb.Event) error {
		fmt.Println("Got event", e)
		return nil
	},
)
if err != nil {
	panic(err)
}
```