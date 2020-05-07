package eventdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/deixis/errors"
	"github.com/deixis/pkg/utc"
	"github.com/deixis/spine/log"
	"github.com/deixis/spine/net/pubsub"
	"github.com/deixis/storage/eventdb/eventpb"
	"github.com/deixis/storage/kvdb"
	"github.com/golang/protobuf/proto"
)

type Subscription interface {
	// Meta returns the subscription metadata
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

type subscriptionWatcher struct {
	mu sync.Mutex

	StreamMeta   *StreamMetadata
	Sub          pubsub.Sub
	SubMeta      *SubscriptionMetadata
	Store        Store
	EventHandler EventHandler

	close chan struct{}
}

func newSubscriptionWatcher() *subscriptionWatcher {
	return &subscriptionWatcher{
		close: make(chan struct{}),
	}
}

func (s *subscriptionWatcher) Meta() (SubscriptionMetadata, error) {
	return *s.SubMeta, nil
}

func (w *subscriptionWatcher) Start(ctx context.Context) {
	log.Trace(ctx, "eventdb.subscription.catchUp", "Catching up",
		log.Uint64("position", w.SubMeta.Position),
		log.Uint64("latest", w.StreamMeta.Version), // latest known version
	)

	// Catching up...
	for {
		// Thought: If we persist events at high frequency, it seems possible that
		// we will never go out of this loop

		select {
		case <-w.close:
			return
		default:
			// Continue
		}

		// Fetch event
		var event *RecordedEvent
		_, err := w.Store.ReadTransact(ctx, func(tx ReadTransaction) (interface{}, error) {
			stream := tx.ReadStream(w.StreamMeta.ID)
			events, err := stream.Events(w.SubMeta.Position+1, WithLimit(1)).GetSliceWithError()
			if err != nil {
				return nil, err
			}
			if len(events) > 0 {
				event = events[0]
			}
			return nil, nil
		})
		if err != nil {
			log.Err(ctx, "eventdb.subscription.catchUp.err", "Transact error",
				log.Error(err),
			)
			return
		}
		if event == nil {
			// Catched up
			break
		}

		// Process it
		if err := w.Process(ctx, event); err != nil {
			log.Err(ctx, "eventdb.subscription.catchUp.err", "Process error",
				log.Error(err),
			)
			return
		}
	}

	// Listen to live events
	log.Trace(ctx, "eventdb.subscription.live", "Listening to live events",
		log.Uint64("position", w.SubMeta.Position),
	)
	err := w.Sub.Subscribe(
		w.SubMeta.Group,
		w.SubMeta.StreamID,
		func(ctx context.Context, data []byte) {
			record := eventpb.RecordedEvent{}
			err := proto.Unmarshal(data, &record)
			if err != nil {
				log.Err(ctx, "eventdb.subscription.live.err",
					"Failed to unmarshal recorded event",
					log.Error(err),
				)
				return
			}

			event := RecordedEvent(record)
			if err := w.Process(ctx, &event); err != nil {
				log.Err(ctx, "eventdb.subscription.live.err",
					"Failed to process event",
					log.Error(err),
				)
				return
			}
		})
	if err != nil {
		log.Err(ctx, "eventdb.subscription.live.err", "Failed to subscribe",
			log.Error(err),
		)
		return
	}

	select {
	case <-w.close:
		return
	}
}

func (w *subscriptionWatcher) Process(
	ctx context.Context, re *RecordedEvent,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	evt, err := re.Unmarshal()
	if err != nil {
		return err
	}

	// Propagate
	if err := w.EventHandler(ctx, evt); err != nil {
		return err
	}

	// Update cursor
	switch w.SubMeta.Type {
	case SubscriptionTypeVolatile, SubscriptionTypeCatchUp:
		return w.updateTransientSub(ctx, re.Number)
	case SubscriptionTypePersistent:
		return w.updatePersistedSub(ctx, re.Number)
	}
	return nil
}

func (w *subscriptionWatcher) updateTransientSub(ctx context.Context, position uint64) error {
	w.SubMeta.Position = position
	w.SubMeta.ModificationTime = utc.Now()
	return nil
}

func (w *subscriptionWatcher) updatePersistedSub(ctx context.Context, position uint64) error {
	var update *SubscriptionMetadata
	_, err := w.Store.Transact(ctx, func(tx Transaction) (interface{}, error) {
		// Fetch latest meta from storage
		stream := tx.Stream(w.StreamMeta.ID)
		sub, err := stream.Subscription(w.SubMeta.Group)
		if err != nil {
			return nil, err
		}
		subMeta, err := sub.Meta()
		if err != nil {
			return nil, err
		}

		// Update fields
		subMeta.Position = position
		subMeta.ModificationTime = utc.Now()

		// Persit back
		// TODO: Call internal function to update?
		data, err := proto.Marshal(subMeta.Pack())
		if err != nil {
			return nil, errors.Wrap(err, "error marshalling subscription meta")
		}
		tx.Transaction().Set(subMeta.Key, data)
		update = &subMeta
		return nil, nil
	})
	if err != nil {
		return err
	}
	w.SubMeta = update
	return nil
}

func (w *subscriptionWatcher) Close() error {
	w.close <- struct{}{}
	return nil
}
