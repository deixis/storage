package eventdb_test

import (
	"context"
	"testing"

	"github.com/deixis/storage/eventdb"
)

func init() {
	eventdb.RegisterEvent((*eventCreated)(nil), "eventdb_test.EventCreated")
	eventdb.RegisterEvent((*eventUpdated)(nil), "eventdb_test.EventUpdated")
}

func TestAggregate_LoadEmptyStream(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	aggr := aggregate{}
	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			if _, err := tx.CreateStream(streamID); err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			aggReader := eventdb.NewAggregateReader(tx.ReadStream(streamID))
			if err := aggReader.Latest(&aggr); err != nil {
				t.Fatal("error loading latest aggregate", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	if len(aggr.events) > 0 {
		t.Errorf("expect to get an empty aggregate, but got %d events", len(aggr.events))
	}
	if aggr.version != 0 {
		t.Errorf("expect to get an empty aggregate, but got version %d", aggr.version)
	}
}

func TestAggregate_Latest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	aggr := aggregate{}
	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			if _, err := tx.CreateStream(streamID); err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			aggregater := eventdb.NewAggregater(tx.Stream(streamID))
			if err := aggregater.Append(0, &eventCreated{}); err != nil {
				t.Fatal("error appending event to aggregate store", err)
			}
			if err := aggregater.Append(1, &eventUpdated{}); err != nil {
				t.Fatal("error appending event to aggregate store", err)
			}
			if err := aggregater.Append(2, &eventUpdated{}); err != nil {
				t.Fatal("error appending event to aggregate store", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			aggReader := eventdb.NewAggregateReader(tx.ReadStream(streamID))
			if err := aggReader.Latest(&aggr); err != nil {
				t.Fatal("error loading latest aggregate", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	var expectVersion uint64 = 3
	if len(aggr.events) != int(expectVersion) {
		t.Errorf("expect to get an aggregate with %d event, but got %d events", expectVersion, len(aggr.events))
	}
	if aggr.version != expectVersion {
		t.Errorf("expect to get an aggregate with version %d, but got version %d", expectVersion, aggr.version)
	}
}

func TestAggregate_LoadVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	aggr := aggregate{}
	var loadVersion uint64 = 2
	withEventDB(t, func(eventStore eventdb.Store) {
		streamID := "alpha"
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			if _, err := tx.CreateStream(streamID); err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			aggregater := eventdb.NewAggregater(tx.Stream(streamID))
			if err := aggregater.Append(0, &eventCreated{}); err != nil {
				t.Fatal("error appending event to aggregate store", err)
			}
			if err := aggregater.Append(1, &eventUpdated{}); err != nil {
				t.Fatal("error appending event to aggregate store", err)
			}
			if err := aggregater.Append(2, &eventUpdated{}); err != nil {
				t.Fatal("error appending event to aggregate store", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
			aggReader := eventdb.NewAggregateReader(tx.ReadStream(streamID))
			if err := aggReader.Load(&aggr, loadVersion); err != nil {
				t.Fatal("error loading latest aggregate", err)
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	if len(aggr.events) != int(loadVersion) {
		t.Errorf("expect to get an aggregate with %d events, but got %d events", loadVersion, len(aggr.events))
	}
	if aggr.version != loadVersion {
		t.Errorf("expect to get an aggregate with version %d, but got version %d", loadVersion, aggr.version)
	}
}

type aggregate struct {
	events  []eventdb.Event
	version uint64
}

func (a *aggregate) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (a *aggregate) UnmarshalBinary(data []byte) error {
	return nil
}

func (a *aggregate) Reduce(e eventdb.Event, version uint64) error {
	a.events = append(a.events, e)
	a.version = version
	return nil
}

type eventCreated struct{}

func (e *eventCreated) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (e *eventCreated) UnmarshalBinary(data []byte) error {
	if e == nil {
		panic("unmarshal on nil pointer")
	}
	return nil
}

type eventUpdated struct{}

func (e *eventUpdated) MarshalBinary() ([]byte, error) {
	return nil, nil
}

func (e *eventUpdated) UnmarshalBinary(data []byte) error {
	if e == nil {
		panic("unmarshal on nil pointer")
	}
	return nil
}
