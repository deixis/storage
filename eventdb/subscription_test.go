package eventdb_test

import (
	"context"
	"testing"

	"github.com/deixis/errors"
	"github.com/deixis/storage/eventdb"
)

func init() {
	eventdb.RegisterEvent((*dummyEvent)(nil), "eventdb_test.dummyEvent")
}

func TestSubscription_CreatePersistedSubscription(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			sub, err := stream.CreateSubscription("foo")
			if err != nil {
				t.Fatal("expect to create stream subscription, but got error", err)
			}
			return sub, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			stream := tx.ReadStream(streamID)

			sub, err := stream.Subscription("foo")
			if err != nil {
				t.Error("expect to load subscription, but got error", err)
			}
			return sub, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestSubscription_Delete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}
			if _, err := stream.CreateSubscription("foo"); err != nil {
				t.Fatal("expect to create stream subscription, but got error", err)
			}
			if err := stream.DeleteSubscription("foo"); err != nil {
				t.Fatal("expect to delete stream subscription, but got error", err)
			}
			return nil, err
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			stream := tx.ReadStream(streamID)

			sub, err := stream.Subscription("foo")
			if err != nil {
				t.Error("expect to load subscription, but got error", err)
			}
			meta, err := sub.Meta()
			if err != nil {
				t.Error("expect to load subscription meta, but got error", err)
			}
			if meta.DeletionTime == 0 {
				t.Error("expect deletion time flag to be set after deletion")
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestSubscription_NotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			if _, err := tx.CreateStream(streamID); err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}
			return nil, err
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			stream := tx.ReadStream(streamID)

			sub, err := stream.Subscription("foo")
			if err != eventdb.ErrSubscriptionNotFound {
				t.Error("expect to get stream not found error, but got", err)
			}
			return sub, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestSubscription_Subscriptions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		subscriptions := []string{"a", "b", "c"}

		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			for _, sub := range subscriptions {
				_, err := stream.CreateSubscription(sub)
				if err != nil {
					t.Fatalf("expect to create stream subscription %s, but got error %s", sub, err)
				}
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			stream := tx.ReadStream(streamID)

			subs, err := stream.Subscriptions().GetSliceWithError()
			if err != nil {
				t.Error("expect to load subscriptions, but got error", err)
			}
			if len(subscriptions) != len(subs) {
				t.Errorf("expect to get %d subscriptions, but %d",
					len(subscriptions), len(subs),
				)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestStore_SubscribeVolatile(t *testing.T) {
	t.Parallel()

	var gotEvents []eventdb.Event
	var expectEvents = []eventdb.Event{
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
	}
	liveEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		ctx := context.Background()

		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}

			event := eventdb.RecordedEvent{
				Name: "foo",
				Data: []byte("bar"),
			}
			if err := stream.AppendEvents(0, &event); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		// Subscribe to stream
		_, err = eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypeVolatile,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}

		// Append "live" events
		_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(streamID)
			if err := stream.AppendEvents(1, liveEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	if len(expectEvents) != len(gotEvents) {
		t.Errorf("expect to get %d events, but %d",
			len(expectEvents), len(gotEvents),
		)
	}
}

// TestStore_SubscribeVolatileResume ensures subscriptions DO NOT restart from
// the beginning for each instance
func TestStore_SubscribeVolatileResume(t *testing.T) {
	t.Parallel()

	var gotEvents []eventdb.Event
	var expectEvents = []eventdb.Event{
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
	}
	events := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}

	streamID := "alpha"
	ctx := context.Background()
	withEventDB(t, func(eventStore eventdb.Store) {
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			_, err = tx.CreateStream(streamID)
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		// Subscribe to stream
		_, err = eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypeVolatile,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}

		// Append events
		_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(streamID)
			if err := stream.AppendEvents(0, events...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	withEventDB(t, func(eventStore eventdb.Store) {
		// Subscribe to stream again
		_, err := eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypeVolatile,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}
	})

	expectEventsLen := len(expectEvents)
	if expectEventsLen != len(gotEvents) {
		t.Errorf("expect to get %d events, but %d",
			expectEventsLen, len(gotEvents),
		)
	}
}

func TestStore_SubscribeCatchUp(t *testing.T) {
	t.Parallel()

	var gotEvents []eventdb.Event
	var expectEvents = []eventdb.Event{
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
	}
	catchUpEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	liveEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		ctx := context.Background()

		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}

			// Append "catch up" events
			if err := stream.AppendEvents(0, catchUpEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		// Subscribe to stream
		_, err = eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypeCatchUp,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}

		// Append "live" events
		_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(streamID)
			expectedVersion := uint64(len(catchUpEvents))
			if err := stream.AppendEvents(expectedVersion, liveEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	if len(expectEvents) != len(gotEvents) {
		t.Errorf("expect to get %d events, but %d",
			len(expectEvents), len(gotEvents),
		)
	}
}

// TestStore_SubscribeCatchUpResume ensures subscriptions restart from the
// beginning for each instance
func TestStore_SubscribeCatchUpResume(t *testing.T) {
	t.Parallel()

	var gotEvents []eventdb.Event
	var expectEvents = []eventdb.Event{
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
	}
	events := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}

	streamID := "alpha"
	ctx := context.Background()
	withEventDB(t, func(eventStore eventdb.Store) {
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			_, err = tx.CreateStream(streamID)
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		// Subscribe to stream
		_, err = eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypeCatchUp,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}

		// Append events
		_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(streamID)
			if err := stream.AppendEvents(0, events...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	withEventDB(t, func(eventStore eventdb.Store) {
		// Subscribe to stream again
		_, err := eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypeCatchUp,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}
	})

	expectEventsLen := 2 * len(expectEvents) // Twice the amount if we resume
	if expectEventsLen != len(gotEvents) {
		t.Errorf("expect to get %d events, but %d",
			expectEventsLen, len(gotEvents),
		)
	}
}

func TestStore_SubscribePersistent(t *testing.T) {
	t.Parallel()

	var gotEvents []eventdb.Event
	var expectEvents = []eventdb.Event{
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
	}
	catchUpEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	liveEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		ctx := context.Background()

		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}

			_, err = stream.CreateSubscription("foo")
			if err != nil {
				return nil, errors.Wrap(err, "expect to create subscription, but got error")
			}

			// Append "catch up" events
			if err := stream.AppendEvents(0, catchUpEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		// Subscribe to stream
		_, err = eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypePersistent,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}

		// Append "live" events
		_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(streamID)
			expectedVersion := uint64(len(catchUpEvents))
			if err := stream.AppendEvents(expectedVersion, liveEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	if len(expectEvents) != len(gotEvents) {
		t.Errorf("expect to get %d events, but %d",
			len(expectEvents), len(gotEvents),
		)
	}
}

// TestStore_SubscribePersistentResume ensures persisted subscriptions pick up
// events where they left off.
func TestStore_SubscribePersistentResume(t *testing.T) {
	t.Parallel()

	streamID := "alpha"
	ctx := context.Background()

	var gotEvents []eventdb.Event
	var expectEvents = []eventdb.Event{
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
		&dummyEvent{},
	}
	catchUpEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	liveEvents := []*eventdb.RecordedEvent{
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
		{Name: "eventdb_test.dummyEvent", Data: []byte{}},
	}
	withEventDB(t, func(eventStore eventdb.Store) {
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream, err := tx.CreateStream(streamID)
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}

			_, err = stream.CreateSubscription("foo")
			if err != nil {
				return nil, errors.Wrap(err, "expect to create subscription, but got error")
			}

			// Append "catch up" events
			if err := stream.AppendEvents(0, catchUpEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			if err != nil {
				return nil, err
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}

		// Subscribe to stream
		_, err = eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypePersistent,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}
	})

	withEventDB(t, func(eventStore eventdb.Store) {
		// Subscribe to stream
		_, err := eventStore.Subscribe(
			ctx,
			streamID,
			"foo",
			eventdb.SubscriptionTypePersistent,
			func(ctx context.Context, e eventdb.Event) error {
				gotEvents = append(gotEvents, e)
				return nil
			},
		)
		if err != nil {
			t.Error("expect to subscribe to stream, but got", err)
			return
		}

		// Append "live" events
		_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(streamID)
			expectedVersion := uint64(len(catchUpEvents))
			if err := stream.AppendEvents(expectedVersion, liveEvents...); err != nil {
				return nil, errors.Wrap(err, "expect to append event, but got error")
			}
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})

	if len(expectEvents) != len(gotEvents) {
		t.Errorf("expect to get %d events, but %d",
			len(expectEvents), len(gotEvents),
		)
	}
}

func TestStore_SubscribeStreamNotFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	withEventDB(t, func(eventStore eventdb.Store) {
		if eventStore == nil {
			panic("BOOM")
		}

		_, err := eventStore.Subscribe(
			ctx,
			"my-stream-key",
			"foo",
			eventdb.SubscriptionTypeVolatile,
			func(ctx context.Context, e eventdb.Event) error {
				return nil
			},
		)
		if err != eventdb.ErrStreamNotFound {
			t.Error("expect to get stream not found error, but got", err)
		}
	})
}

type dummyEvent struct{}

func (e *dummyEvent) MarshalEvent() ([]byte, error) {
	return nil, nil
}

func (e *dummyEvent) UnmarshalEvent(data []byte) error {
	if e == nil {
		panic("unmarshal on nil pointer")
	}
	return nil
}
