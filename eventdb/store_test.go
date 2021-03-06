package eventdb_test

import (
	"context"
	"path"
	"testing"

	"github.com/deixis/errors"
	"github.com/deixis/storage/eventdb"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/driver/bbolt"
	"github.com/deixis/storage/kvdb/kvtrace"
)

func TestDB_Init(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	var transactCalled bool
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		transactCalled = true
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !transactCalled {
		t.Error("expect transaction to be called, but got nothing")
	}

	var readTransactCalled bool
	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		readTransactCalled = true
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !readTransactCalled {
		t.Error("expect read transaction to be called, but got nothing")
	}
}

func TestStream_Create(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}

		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to load stream metadata, but got", err)
		}
		if meta.ID != streamID {
			t.Errorf("expect to get stream ID %s on metadata, but got %s", streamID, meta.ID)
		}

		_, err = tx.CreateStream(streamID)
		if !errors.IsAborted(err) {
			t.Error("expect to get an ID conflict when creating the stream, but got", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		stream := tx.ReadStream(streamID)
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to load stream metadata, but got", err)
		}
		if meta.ID != streamID {
			t.Errorf("expect to get stream ID %s on metadata, but got %s", streamID, meta.ID)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStream_Events(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}

		event := eventdb.RecordedEvent{
			Name: "foo.bar",
			Data: []byte("day taaa"),
			Meta: []byte(""),
		}
		if err = stream.AppendEvents(0, &event); err != nil {
			t.Fatal("expect to append event, but got error", err)
		}
		event = eventdb.RecordedEvent{
			Name: "foo.bar",
			Data: []byte("day taaa"),
			Meta: []byte(""),
		}
		if err = stream.AppendEvents(1, &event); err != nil {
			t.Fatal("expect to append event, but got error", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		stream := tx.ReadStream(streamID)
		events, err := stream.Events(0).GetSliceWithError()
		if err != nil {
			t.Fatal("expect to read events, but got", err)
		}
		expectEvents := 2
		if expectEvents != len(events) {
			t.Errorf("expect to get %d events, but got %d", expectEvents, len(events))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStream_Snapshot(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}

		event := eventdb.RecordedEvent{
			Name: "foo.bar",
			Data: []byte("day taaa"),
			Meta: []byte(""),
		}
		if err = stream.AppendEvents(0, &event); err != nil {
			t.Fatal("expect to append event, but got error", err)
		}
		event = eventdb.RecordedEvent{
			Name: "foo.bar",
			Data: []byte("day taaa"),
			Meta: []byte(""),
		}
		if err = stream.AppendEvents(1, &event); err != nil {
			t.Fatal("expect to append event, but got error", err)
		}

		snap := eventdb.RecordedSnapshot{
			Data: []byte("snap day ta"),
		}
		if err = stream.SetSnapshot(0, &snap); err != nil {
			t.Fatal("expect to set snapshot, but got error", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		stream := tx.ReadStream(streamID)
		events, err := stream.Events(0).GetSliceWithError()
		if err != nil {
			t.Fatal("expect to read events, but got", err)
		}
		expectEvents := 2
		if expectEvents != len(events) {
			t.Errorf("expect to get %d events, but got %d", expectEvents, len(events))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStream_Isolation(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	otherID := "beta"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		other, err := tx.CreateStream(otherID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}

		event := eventdb.RecordedEvent{
			Name: "foo.bar",
			Data: []byte("day taaa"),
			Meta: []byte(""),
		}
		if err = other.AppendEvents(0, &event); err != nil {
			t.Fatal("expect to append event, but got error", err)
		}
		event = eventdb.RecordedEvent{
			Name: "foo.bar",
			Data: []byte("day taaa"),
			Meta: []byte(""),
		}
		if err = other.AppendEvents(1, &event); err != nil {
			t.Fatal("expect to append event, but got error", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		stream := tx.ReadStream(streamID)
		events, err := stream.Events(0).GetSliceWithError()
		if err != nil {
			t.Fatal("expect to read events, but got", err)
		}
		expectEvents := 0
		if expectEvents != len(events) {
			t.Errorf("expect to get %d events, but got %d", expectEvents, len(events))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStream_SoftDeletion(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}
		if err := stream.Delete(); err != nil {
			t.Fatal("expect to delete stream, but got", err)
		}
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}
		if meta.DeletionTime == 0 {
			t.Error("expect to get have a deleted stream")
		}

		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		stream := tx.ReadStream(streamID)
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}
		if meta.DeletionTime == 0 {
			t.Error("expect to get have a deleted stream")
		}

		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStream_Restoration(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}
		if err := stream.Delete(); err != nil {
			t.Fatal("expect to delete stream, but got", err)
		}
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}
		if meta.DeletionTime == 0 {
			t.Error("expect to get have a deleted stream")
		}

		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream := tx.Stream(streamID)
		if err := stream.Restore(); err != nil {
			t.Fatal("expect to stream stream, but got", err)
		}
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}
		if meta.DeletionTime != 0 {
			t.Error("expect to get have a restored stream")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ensure stream is reindexed
	_, err = eventStore.ReadTransact(ctx, func(tx eventdb.ReadTransaction) (v interface{}, err error) {
		streams, err := tx.ReadStreams().GetSliceWithError()
		if err != nil {
			t.Fatal("unexpected streams error", err)
		}
		if len(streams) != 1 {
			t.Fatal("expect to retrieve the restored stream, but got", len(streams))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Attempt to restore a non-deleted stream
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream := tx.Stream(streamID)
		if err := stream.Restore(); err != nil {
			t.Fatal("unexpected restore error on a non-deleted stream", err)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestStream_EmptyExtendedMeta ensures extended metadata are empty by default
func TestStream_EmptyExtendedMeta(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}

		if len(meta.Extended) > 0 {
			t.Fatal("expect to get no extended metadata, but got", meta.Extended)
		}

		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestStream_CreationExtendedMeta adds extended metadata upon creation
func TestStream_CreationExtendedMeta(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	expectMeta := map[string]string{
		"Idempotency-Key": "123",
		"Version":         "atmos",
	}
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID, expectMeta)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}
		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}

		if len(expectMeta) != len(meta.Extended) {
			t.Fatalf("expect to get %d metadata, but got %d", len(expectMeta), len(meta.Extended))
		}

		if err := stream.DeleteExtendedMeta("Idempotency-Key", "Version"); err != nil {
			t.Fatal("expect to delete stream metadata, but got", err)
		}

		meta, err = stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}

		if len(meta.Extended) > 0 {
			t.Fatalf("expect to metada to be deleted, but got %d", len(meta.Extended))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestStream_SetExtendedMeta sets extended metadata after creation
func TestStream_SetExtendedMeta(t *testing.T) {
	t.Parallel()

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}
	eventStore, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	streamID := "alpha"
	expectMeta := map[string]string{
		"Idempotency-Key": "123",
		"Version":         "atmos",
	}
	_, err = eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
		stream, err := tx.CreateStream(streamID)
		if err != nil {
			t.Fatal("expect to create stream, but got error", err)
		}
		if err := stream.SetExtendedMeta(expectMeta); err != nil {
			t.Fatal("expect to set extended metadata, but got error", err)
		}

		meta, err := stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}

		if len(expectMeta) != len(meta.Extended) {
			t.Fatalf("expect to get %d metadata, but got %d", len(expectMeta), len(meta.Extended))
		}

		if err := stream.DeleteExtendedMeta("Idempotency-Key", "Version"); err != nil {
			t.Fatal("expect to delete stream metadata, but got", err)
		}

		meta, err = stream.Metadata()
		if err != nil {
			t.Fatal("expect to get stream metadata, but got", err)
		}

		if len(meta.Extended) > 0 {
			t.Fatalf("expect to metada to be deleted, but got %d", len(meta.Extended))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
