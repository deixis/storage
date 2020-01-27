package eventdb_test

import (
	"context"
	"testing"

	"github.com/deixis/storage/eventdb"
	"github.com/deixis/storage/kvdb"
)

func TestStream_Range(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withKV(t, func(kvs kvdb.Store, dir kvdb.DirectorySubspace) {
		streamIDs := []string{
			"alpha",
			"beta",
		}

		_, err := kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
			eventx, err := eventdb.Transact(tx, dir)
			if err != nil {
				return nil, err
			}
			for _, streamID := range streamIDs {
				if _, err := eventx.CreateStream(streamID); err != nil {
					t.Fatal("expect to create stream, but got error", err)
				}

				aggregater := eventdb.NewAggregater(eventx.Stream(streamID))
				if err := aggregater.Append(0, &eventCreated{}); err != nil {
					t.Fatal("error appending event to aggregate store", err)
				}
				if err := aggregater.Append(1, &eventUpdated{}); err != nil {
					t.Fatal("error appending event to aggregate store", err)
				}
				if err := aggregater.Append(2, &eventUpdated{}); err != nil {
					t.Fatal("error appending event to aggregate store", err)
				}
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = kvs.ReadTransact(ctx, func(tx kvdb.ReadTransaction) (v interface{}, err error) {
			eventx, err := eventdb.ReadTransact(tx, dir)
			if err != nil {
				return nil, err
			}
			res := eventx.ReadStreams()
			streams, err := res.GetSliceWithError()
			if err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			if len(streamIDs) != len(streams) {
				t.Errorf("expect to get %d streams, but got %d", len(streamIDs), len(streams))
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestStream_RangeAfterSoftDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withKV(t, func(kvs kvdb.Store, dir kvdb.DirectorySubspace) {
		streamIDs := []string{
			"alpha",
			"beta",
		}

		_, err := kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
			eventx, err := eventdb.Transact(tx, dir)
			if err != nil {
				return nil, err
			}
			for _, streamID := range streamIDs {
				stream, err := eventx.CreateStream(streamID)
				if err != nil {
					t.Fatal("expect to create stream, but got error", err)
				}
				if err := stream.Delete(); err != nil {
					t.Fatal("expect to delete stream, but got error", err)
				}
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = kvs.ReadTransact(ctx, func(tx kvdb.ReadTransaction) (v interface{}, err error) {
			eventx, err := eventdb.ReadTransact(tx, dir)
			if err != nil {
				return nil, err
			}
			res := eventx.ReadStreams()
			streams, err := res.GetSliceWithError()
			if err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			expectStreams := 0
			if expectStreams != len(streams) {
				t.Errorf("expect to get %d streams, but got %d", expectStreams, len(streams))
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestStream_RangeAfterPermanentDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	withKV(t, func(kvs kvdb.Store, dir kvdb.DirectorySubspace) {
		streamIDs := []string{
			"alpha",
			"beta",
		}

		_, err := kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
			eventx, err := eventdb.Transact(tx, dir)
			if err != nil {
				return nil, err
			}
			for _, streamID := range streamIDs {
				stream, err := eventx.CreateStream(streamID)
				if err != nil {
					t.Fatal("expect to create stream, but got error", err)
				}
				stream.PermanentlyDelete()
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}

		_, err = kvs.ReadTransact(ctx, func(tx kvdb.ReadTransaction) (v interface{}, err error) {
			eventx, err := eventdb.ReadTransact(tx, dir)
			if err != nil {
				return nil, err
			}
			res := eventx.ReadStreams()
			streams, err := res.GetSliceWithError()
			if err != nil {
				t.Fatal("expect to create stream, but got error", err)
			}

			expectStreams := 0
			if expectStreams != len(streams) {
				t.Errorf("expect to get %d streams, but got %d", expectStreams, len(streams))
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}
