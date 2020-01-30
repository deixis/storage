# EventDB - Event store

## Introduction

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

**There is no Delete**

It is not possible to jump into the time machine and say that an event never
occurred (eg: delete a previous event).
As such, it is necessary to model a delete explicitly as a new transaction.
e.g. InvoiceCreated, InvoiceUpdated, and InvoiceDeleted

## Aggregate

An Aggregate is a cluster of domain objects that can be treated as a single unit.
An aggregate state is a projection of all of its events.

The majority of the application’s business logic is implemented by aggregates.
An aggregate does two things:
- Processes commands and returns events, which leaves the state of the
	aggregate unchanged.
- Consumes events, which updates its state.

Aggregate comes from the DDD (domain-driver design) terminology

## Snapshot

A snapshot is a projection of the current state of an aggregate at a given point. It represents the state when all events to that point have been replayed. You use snapshots as a heuristic to prevent the need to load all events for the entire history of an aggregate. One way of processing events in the event stream is to replay the events from the beginning of time until the end of the event stream.

The problem is that there may be a large number of events between the beginning of time and the current point. You can imagine that an event stream with a million or more events would be inefficient to load.

The solution is to use a snapshot to place a denormalisation of the state at a given point. It is then possible to play the events from that point forward to load the aggregate.

## Subscription

Subscriptions support the "competing consumers" messaging pattern. That means
it is possible to have a group of consumers that competes on the same subscription,
with each subscription getting an at-least-once guarantee. It is particularily
useful on a distributed environment.

### Types

**Volatile**

Volatile subscription calls a given function for events written after
establishing the subscription. They are useful when you need notification
of new events with minimal latency, but where it's not necessary to process
every event.

For example, if a stream has 100 events in it when a subscriber connects,
the subscriber can expect to see event number 101 onwards until the time
the subscription is closed or dropped.

**Catch up**

Catch up subscription specifies a starting point, in the form of an event
number or transaction file position. You call the function for events from
the starting point until the end of the stream, and then for subsequently
written events.

For example, if you specify a starting point of 50 when a stream has 100
events, the subscriber can expect to see events 51 through 100, and then
any events are subsequently written until you drop or close the
subscription.

**Persistent**

Persistent subscription saves the subscription state on EventDB, so consumers
don't have to keep track of processed events on a stream.

Note: Persistent require to create a persistent subscription first.

```go
stream, err := tx.CreateStream("my-stream")
if err != nil {
	panic(err)
}

// CreateSubscription creates a persistent subscription for `my-stream`
sub, err := stream.CreateSubscription("my-subscription-group")
if err != nil {
	panic(err)
}
```

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

## Credits

This package is heavily inspired from [Event Store](https://eventstore.org/).