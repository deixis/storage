package eventdb

import (
	"context"
	"fmt"

	"github.com/deixis/pkg/utc"
	"github.com/deixis/storage/eventdb/eventpb"
	"github.com/deixis/storage/kvdb"
)

type Subscription interface {
	Meta() (SubscriptionMetadata, error)
}

type SubscriptionMetadata struct {
	// Key is the canonical stream identifier
	Key kvdb.Key
	// Group is the unique stream identifier within its scope (stream)
	Group string
	// StreamID is the stream unique identifier
	StreamID string
	// Type defines the subscription type (volatile, catch up, persistent)
	Type SubscriptionType

	// CreationTime is the date on which the stream was created
	CreationTime utc.UTC
	// ModificationTime is the date on which the stream was modified
	ModificationTime utc.UTC
	// DeletionTime is the date on which the stream was soft deleted
	DeletionTime utc.UTC

	// Position is the current position in the stream the subscription
	Position uint64

	// Extended properties
	Extended map[string]string
}

func (m *SubscriptionMetadata) Pack() *eventpb.SubscriptionMetadata {
	return &eventpb.SubscriptionMetadata{
		Key:              m.Key,
		Group:            m.Group,
		StreamID:         m.StreamID,
		Type:             m.Type.Pack(),
		Position:         m.Position,
		CreationTime:     int64(m.CreationTime),
		ModificationTime: int64(m.ModificationTime),
		DeletionTime:     int64(m.DeletionTime),
		Extended:         m.Extended,
	}
}

func (m *SubscriptionMetadata) Unpack(s *eventpb.SubscriptionMetadata) {
	m.Key = s.Key
	m.Group = s.Group
	m.StreamID = s.StreamID
	m.Type.Unpack(s.Type)
	m.Position = s.Position
	m.CreationTime = utc.UTC(s.CreationTime)
	m.ModificationTime = utc.UTC(s.ModificationTime)
	m.DeletionTime = utc.UTC(s.DeletionTime)
	m.Extended = s.Extended
}

type SubscriptionType string

func (t SubscriptionType) Pack() eventpb.SubscriptionType {
	switch t {
	case SubscriptionTypeCatchUp:
		return eventpb.SubscriptionType_CatchUp
	case SubscriptionTypePersistent:
		return eventpb.SubscriptionType_Persistent
	case SubscriptionTypeVolatile:
		return eventpb.SubscriptionType_Volatile
	default:
		panic(fmt.Sprintf("unmapped subscription type <%s>", t))
	}
}

func (t *SubscriptionType) Unpack(st eventpb.SubscriptionType) {
	switch st {
	case eventpb.SubscriptionType_CatchUp:
		*t = SubscriptionTypeCatchUp
	case eventpb.SubscriptionType_Persistent:
		*t = SubscriptionTypePersistent
	case eventpb.SubscriptionType_Volatile:
		*t = SubscriptionTypeVolatile
	default:
		panic(fmt.Sprintf("unmapped subscription type <%s>", st))
	}
}

const (
	// SubscriptionTypeVolatile calls a given function for events written after
	// establishing the subscription. They are useful when you need notification
	// of new events with minimal latency, but where it's not necessary to process
	// every event.
	//
	// For example, if a stream has 100 events in it when a subscriber connects,
	// the subscriber can expect to see event number 101 onwards until the time
	// the subscription is closed or dropped.
	SubscriptionTypeVolatile = "volatile"
	// SubscriptionTypeCatchUp specifies a starting point, in the form of an event
	// number or transaction file position. You call the function for events from
	// the starting point until the end of the stream, and then for subsequently
	// written events.
	//
	// For example, if you specify a starting point of 50 when a stream has 100
	// events, the subscriber can expect to see events 51 through 100, and then
	// any events are subsequently written until you drop or close the
	// subscription.
	SubscriptionTypeCatchUp = "catchUp"
	// SubscriptionTypePersistent supports the "competing consumers" messaging
	// pattern and are useful when you need to distribute messages to many workers.
	//
	// EventDB saves the subscription state and allows for at-least-once delivery
	// guarantees across multiple consumers on the same stream.
	// It is possible to have many groups of consumers compete on the same stream,
	// with each group getting an at-least-once guarantee.
	SubscriptionTypePersistent = "persistent"
)

type subscription struct {
	meta SubscriptionMetadata
}

func (s *subscription) Meta() (SubscriptionMetadata, error) {
	return s.meta, nil
}

type volatileSubscription struct {
	Stream       StreamMetadata
	Group        string
	CreationTime utc.UTC
}

func newVolatileSubscription(meta *StreamMetadata, group string) Subscription {
	return &volatileSubscription{
		Stream:       *meta,
		Group:        group,
		CreationTime: utc.Now(),
	}
}

func (s *volatileSubscription) Meta() (SubscriptionMetadata, error) {
	return SubscriptionMetadata{
		Key:              nil,
		Group:            s.Group,
		StreamID:         s.Stream.ID,
		Type:             SubscriptionTypeVolatile,
		CreationTime:     s.CreationTime,
		ModificationTime: s.CreationTime,
		DeletionTime:     0,
		Position:         s.Stream.Version,
		Extended:         map[string]string{},
	}, nil
}

type catchUpSubscription struct {
	Stream       StreamMetadata
	Group        string
	CreationTime utc.UTC
}

func newCatchUpSubscription(meta *StreamMetadata, group string) Subscription {
	return &catchUpSubscription{
		Stream:       *meta,
		Group:        group,
		CreationTime: utc.Now(),
	}
}

func (s *catchUpSubscription) Meta() (SubscriptionMetadata, error) {
	return SubscriptionMetadata{
		Key:              nil,
		Group:            s.Group,
		StreamID:         s.Stream.ID,
		Type:             SubscriptionTypeCatchUp,
		CreationTime:     s.CreationTime,
		ModificationTime: s.CreationTime,
		DeletionTime:     0,
		Position:         0,
		Extended:         map[string]string{},
	}, nil
}

type publish interface {
	Pub(ctx context.Context, e Event) error
}
