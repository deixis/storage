package eventdb_test

import (
	"context"
	"testing"

	"github.com/deixis/storage/eventdb"
)

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
