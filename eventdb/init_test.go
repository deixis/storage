package eventdb_test

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/deixis/errors"
	"github.com/deixis/spine/log"
	stesting "github.com/deixis/spine/testing"
	"github.com/deixis/storage/eventdb"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/driver/bbolt"
	"github.com/deixis/storage/kvdb/kvtrace"
)

func TestMain(m *testing.M) {
	os.Mkdir("db", 0770)

	var code int
	func() {
		defer os.RemoveAll("db")
		code = m.Run()
	}()

	os.Exit(code)
}

func withStream(t *testing.T, id string, fn func(eventdb.Store, eventdb.Stream)) {
	withEventDB(t, func(eventStore eventdb.Store) {
		ctx := context.Background()
		_, err := eventStore.Transact(ctx, func(tx eventdb.Transaction) (v interface{}, err error) {
			stream := tx.Stream(id)

			_, err = stream.Metadata()
			switch err {
			case eventdb.ErrStreamNotFound:
				stream, err = tx.CreateStream(id)
			}
			if err != nil {
				return nil, errors.Wrap(err, "expect to create stream, but got error")
			}

			fn(eventStore, stream)
			return nil, nil
		})
		if err != nil {
			t.Error(err)
			return
		}
	})
}

func withEventDB(t *testing.T, fn func(eventdb.Store)) {
	ctx := context.TODO()
	ctx = log.WithContext(ctx, stesting.NewLogger(t, true))

	// Create database client
	bbs, err := bbolt.Open(path.Join("./db", t.Name()), 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}

	kvs := kvtrace.Trace(bbs)
	defer kvs.Close()

	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}

	s, err := eventdb.New(kvs, dir)
	if err != nil {
		t.Fatal(err)
	}

	// Callback
	fn(s)

	// Wait for messages to process
	time.Sleep(time.Second)

	// Close store
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func withKV(t *testing.T, fn func(kvdb.Store, kvdb.DirectorySubspace)) {
	// Create database client
	bbs, err := bbolt.Open("./db/testing", 0600, t.Name())
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.Background()

	kvs := kvtrace.Trace(bbs)
	defer kvs.Close()

	kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		// Just a test
		return nil, nil
	})
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		t.Fatal(err)
	}

	fn(kvs, dir)
}
