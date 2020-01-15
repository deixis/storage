package eventdb

import (
	"encoding"

	"github.com/segmentio/ksuid"
)

// An Aggregate is a cluster of domain objects that can be treated as
// a single unit.
// An aggregate state is a projection of all of its events.
//
// The majority of the application’s business logic is implemented by aggregates.
// An aggregate does two things:
//  - Processes commands and returns events, which leaves the state of the
//		aggregate unchanged.
//  - Consumes events, which updates its state.
//
// Aggregate comes from the DDD (domain-driver design) terminology
type Aggregate interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler

	// Reduce applies the event to the aggregate, which will update its internal
	// state.
	//
	// Note: Reduce must verify that an event is applicable in its current state.
	// The event store will replay all persisted events and then the new ones
	// before persisting them in order to ensure they do not put the aggregate
	// in an inconsistent state.
	// Once validated, the events are added to the stream and persisted to the
	// database. Should any other events have occured on the stream,
	// the event store will pull them and start the process again or return a
	// 409 (conflict) depending on the strategy.
	// Changes on a stream are guaranteed to be consistent.
	Reduce(e Event, version uint64) error
}

// AggregateReader simplifies interaction with a Stream used for storing Aggregate
type AggregateReader interface {
	Latest(Aggregate) error
	Load(a Aggregate, version uint64) error
}

// AggregateWriter simplifies interaction with a Stream used for storing Aggregate
type AggregateWriter interface {
	Append(expectedVersion uint64, e Event) error
}

// Aggregater simplifies interaction with a Stream used for storing Aggregate
type Aggregater interface {
	AggregateReader
	AggregateWriter
}

// NewAggregateReader builds a new AggregateReader
func NewAggregateReader(stream StreamReader) AggregateReader {
	return &aggregateReadTransaction{stream: stream}
}

// NewAggregater builds a new Aggregater
func NewAggregater(stream Stream) Aggregater {
	a := &aggregateTransaction{stream: stream}
	a.aggregateReadTransaction.stream = stream
	return a
}

type aggregateReadTransaction struct {
	stream StreamReader
}

func (tx *aggregateReadTransaction) Latest(a Aggregate) error {
	meta, err := tx.stream.Metadata()
	if err != nil {
		return err
	}
	return tx.load(a, &meta, meta.Version)
}

func (tx *aggregateReadTransaction) Load(a Aggregate, version uint64) error {
	meta, err := tx.stream.Metadata()
	if err != nil {
		return err
	}
	return tx.load(a, &meta, version)
}

func (tx *aggregateReadTransaction) load(a Aggregate, meta *StreamMetadata, version uint64) error {
	// Events in range
	var from uint64
	var to = version

	// Attempt to start loading events from latest snapshot
	recordedSnap, err := tx.stream.ClosestSnapshot(version)
	switch err {
	case nil:
		// Continue from snapshot version
		from = recordedSnap.Number
		if err := a.UnmarshalBinary(recordedSnap.Data); err != nil {
			return err
		}
	case ErrNoSnapshot:
		// Ignore, rebuild snapshot from scratch
	default:
		return err
	}

	// Short-circuit when the aggregate is already at the correct version
	if from == to {
		return nil
	}

	// Apply remaining events
	iter := tx.stream.EventsInRange(from, to).Iterator()
	for iter.Advance() {
		recordedEvent, err := iter.Get()
		if err != nil {
			return err
		}
		event, err := recordedEvent.Unmarshal()
		if err != nil {
			return err
		}
		if err := a.Reduce(event, recordedEvent.Number); err != nil {
			return err
		}
	}
	return nil
}

type aggregateTransaction struct {
	aggregateReadTransaction

	stream Stream
}

func (tx *aggregateTransaction) Append(expectedVersion uint64, e Event) error {
	meta, err := tx.stream.Metadata()
	if err != nil {
		return err
	}
	data, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	rEvent := RecordedEvent{
		ID:     ksuid.New().String(),
		Number: meta.Version + 1,
		Name:   EventName(e),
		Data:   data,
		Meta:   nil,
	}
	return tx.stream.AppendEvents(expectedVersion, &rEvent)
}

func (tx *aggregateTransaction) SetSnapshot(version uint64, a Aggregate) error {
	data, err := a.MarshalBinary()
	if err != nil {
		return err
	}
	rSnap := RecordedSnapshot{
		ID:     ksuid.New().String(),
		Number: version,
		Data:   data,
		Meta:   nil,
	}
	return tx.stream.SetSnapshot(version, &rSnap)
}
