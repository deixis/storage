package bbolt_test

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/deixis/errors"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/driver/bbolt"
)

func TestMain(m *testing.M) {
	os.MkdirAll("db", 0770)
	s := m.Run()
	os.RemoveAll("db")
	os.Exit(s)
}

func TestOpenClose(t *testing.T) {
	store := openDB(t)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWriteThenRead writes then read data from the same transaction
func TestWriteThenRead(t *testing.T) {
	store := openDB(t)

	expect := []byte("bar")

	got, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Set(kvdb.Key("foo"), expect)
		s := tx.Get(kvdb.Key("foo"))
		return s.Get()
	})
	if err != nil {
		t.Error(err)
	}
	if got == nil {
		t.Error("expect v to be different than nil")
	}
	if string(expect) != string(got.([]byte)) {
		t.Errorf("expect %s, but got %s", expect, got)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestWriteCommitThenRead writes, commit data, and then read data in another tx
func TestWriteCommitThenRead(t *testing.T) {
	store := openDB(t)

	expect := []byte("bar")

	got, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Set(kvdb.Key("foo"), expect)
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("expect v to be nil, but got %v", got)
	}

	got, err = store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.Get(kvdb.Key("foo")).Get()
	})
	if err != nil {
		t.Error(err)
	}
	if got == nil {
		t.Error("expect v to be different than nil")
	}
	if string(expect) != string(got.([]byte)) {
		t.Errorf("expect %s, but got %s", expect, got)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteClearThenRead(t *testing.T) {
	store := openDB(t)

	got, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Set(kvdb.Key("foo"), []byte("bar"))
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("expect v to be nil, but got %v", got)
	}

	got, err = store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Clear(kvdb.Key("foo"))
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("expect v to be nil, but got %v", got)
	}

	got, err = store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.Get(kvdb.Key("foo")).Get()
	})
	if err != nil {
		t.Error(err)
	}
	if got == nil {
		t.Error("expect v to be different than nil")
	}
	if len(got.([]byte)) > 0 {
		t.Errorf("expect empty data, but got %s", got)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetRange(t *testing.T) {
	store := openDB(t)

	entries := []string{
		"alpha/123",
		"beta/123",
		"beta/456",
		"beta/789",
		"gamma/123",
	}

	_, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		for _, entry := range entries {
			tx.Set(kvdb.Key(entry), nil)
		}
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}

	res, err := store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("a"),
			End:   kvdb.Key("z"),
		}).GetSliceWithError()
	})
	if err != nil {
		t.Error(err)
	}
	elems, ok := res.([]kvdb.KeyValue)
	if !ok {
		t.Fatalf("expect to get KeyValue, but got %v", elems)
	}
	if len(entries) != len(elems) {
		t.Fatalf("expect to get %d entries, but got %d", len(entries), len(elems))
	}
	for i := range elems {
		if entries[i] != string(elems[i].Key) {
			t.Errorf("expect key %s, but got %s", entries[i], elems[i].Key)
		}
	}

	res, err = store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("alpha/123"),
			End:   kvdb.Key("gamma/123"),
		}).GetSliceWithError()
	})
	if err != nil {
		t.Error(err)
	}
	elems, ok = res.([]kvdb.KeyValue)
	if !ok {
		t.Fatalf("expect to get KeyValue, but got %v", elems)
	}
	if len(entries) != len(elems) {
		t.Fatalf("expect to get %d entries, but got %d", len(entries), len(elems))
	}
	for i := range elems {
		if entries[i] != string(elems[i].Key) {
			t.Errorf("expect key %s, but got %s", entries[i], elems[i].Key)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetRangeLimit(t *testing.T) {
	store := openDB(t)

	entries := []string{
		"alpha/123",
		"beta/123",
		"beta/456",
		"beta/789",
		"gamma/123",
	}

	_, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		for _, entry := range entries {
			tx.Set(kvdb.Key(entry), nil)
		}
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}

	for i := 1; i <= 7; i++ {
		res, err := store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
			return tx.GetRange(kvdb.KeyRange{
				Begin: kvdb.Key("a"),
				End:   kvdb.Key("z"),
			}, kvdb.WithRangeLimit(i)).GetSliceWithError()
		})
		if err != nil {
			t.Error(err)
		}
		elems, ok := res.([]kvdb.KeyValue)
		if !ok {
			t.Fatalf("expect to get KeyValue, but got %v", elems)
		}
		expect := i
		if expect > 5 {
			expect = 5
		}
		if expect != len(elems) {
			t.Fatalf("expect to get %d entries, but got %d", expect, len(elems))
		}
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetRangeReverse(t *testing.T) {
	store := openDB(t)

	entries := []string{
		"alpha/123",
		"beta/123",
		"beta/456",
		"beta/789",
		"gamma/123",
	}

	_, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		for _, entry := range entries {
			tx.Set(kvdb.Key(entry), nil)
		}
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}

	res, err := store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("a"),
			End:   kvdb.Key("z"),
		}, kvdb.WithRangeReverse(true)).GetSliceWithError()
	})
	if err != nil {
		t.Error(err)
	}
	elems, ok := res.([]kvdb.KeyValue)
	if !ok {
		t.Fatalf("expect to get KeyValue, but got %v", elems)
	}
	if len(entries) != len(elems) {
		t.Fatalf("expect to get %d entries with broad range, but got %d", len(entries), len(elems))
	}
	for i := range elems {
		if entries[len(entries)-1-i] != string(elems[i].Key) {
			t.Errorf("expect key %s, but got %s", entries[len(entries)-1-i], elems[i].Key)
		}
	}

	// With exact match
	res, err = store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("alpha/123"),
			End:   kvdb.Key("gamma/123"),
		}, kvdb.WithRangeReverse(true)).GetSliceWithError()
	})
	if err != nil {
		t.Error(err)
	}
	elems, ok = res.([]kvdb.KeyValue)
	if !ok {
		t.Fatalf("expect to get KeyValue, but got %v", elems)
	}
	if len(entries) != len(elems) {
		t.Fatalf("expect to get %d entries with exact match, but got %d", len(entries), len(elems))
	}
	for i := range elems {
		if entries[len(entries)-1-i] != string(elems[i].Key) {
			t.Errorf("expect key %s, but got %s", entries[len(entries)-1-i], elems[i].Key)
		}
	}

	// Exact match on a sub-slice
	sub := []string{
		"beta/123",
		"beta/456",
		"beta/789",
	}
	res, err = store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		return tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("beta/123"),
			End:   kvdb.Key("beta/789"),
		}, kvdb.WithRangeReverse(true)).GetSliceWithError()
	})
	if err != nil {
		t.Error(err)
	}
	elems, ok = res.([]kvdb.KeyValue)
	if !ok {
		t.Fatalf("expect to get KeyValue, but got %v", elems)
	}
	if len(sub) != len(elems) {
		t.Fatalf("expect to get %d with exact match on sub-slice, but got %d", len(sub), len(elems))
	}
	for i := range elems {
		if sub[len(sub)-1-i] != string(elems[i].Key) {
			t.Errorf("expect key %s on sub-slice, but got %s", sub[len(sub)-1-i], elems[i].Key)
		}
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClearRange(t *testing.T) {
	store := openDB(t)

	got, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Set(kvdb.Key("alpha/123"), nil)
		tx.Set(kvdb.Key("beta/123"), nil)
		tx.Set(kvdb.Key("beta/456"), nil)
		tx.Set(kvdb.Key("beta/789"), nil)
		tx.Set(kvdb.Key("gamma/123"), nil)

		res := tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("a"),
			End:   kvdb.Key("z"),
		})
		elems, err := res.GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(elems) != 5 {
			t.Errorf("expect to get %d elements, but got %d", 5, len(elems))
		}
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("expect v to be nil, but got %v", got)
	}

	got, err = store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		// Out of range
		tx.ClearRange(kvdb.KeyRange{
			Begin: kvdb.Key("alpha/0"),
			End:   kvdb.Key("alpha/1"),
		})
		// Range to delete
		tx.ClearRange(kvdb.KeyRange{
			Begin: kvdb.Key("beta/0"),
			End:   kvdb.Key("beta/z"),
		})
		// Out of range
		tx.ClearRange(kvdb.KeyRange{
			Begin: kvdb.Key("gamma/2"),
			End:   kvdb.Key("gamma/z"),
		})
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("expect v to be nil, but got %v", got)
	}

	_, err = store.ReadTransact(context.Background(), func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		res := tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("a"),
			End:   kvdb.Key("z"),
		})
		elems, err := res.GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(elems) != 2 {
			t.Errorf("expect to get %d elements, but got %d", 2, len(elems))
		}
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSubspace(t *testing.T) {
	store := openDB(t)

	ss, err := store.CreateOrOpenDir([]string{"subsub"})
	if err != nil {
		t.Fatal(err)
	}

	got, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Set(ss.Pack(kvdb.Tuple{"alpha", 123}), nil)
		tx.Set(ss.Pack(kvdb.Tuple{"beta", 123}), nil)
		tx.Set(ss.Pack(kvdb.Tuple{"beta", 456}), nil)
		tx.Set(ss.Pack(kvdb.Tuple{"beta", 789}), nil)
		tx.Set(ss.Pack(kvdb.Tuple{"gamma", 123}), nil)

		res := tx.GetRange(kvdb.KeyRange{
			Begin: ss.Pack(kvdb.Tuple{"a"}),
			End:   ss.Pack(kvdb.Tuple{"z"}),
		})
		elems, err := res.GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(elems) != 5 {
			t.Errorf("expect to get %d elements, but got %d", 5, len(elems))
		}

		var i int
		iterator := res.Iterator()
		for iterator.Advance() {
			i++
		}
		if len(elems) != 5 {
			t.Errorf("expect to get %d elements in iterator, but got %d", 5, len(elems))
		}

		// Test same range without subspace
		res = tx.GetRange(kvdb.KeyRange{
			Begin: kvdb.Key("a"),
			End:   kvdb.Key("z"),
		})
		elems, err = res.GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(elems) != 0 {
			t.Errorf("expect to get no elements, but got %d", len(elems))
		}
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if got != nil {
		t.Errorf("expect v to be nil, but got %v", got)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAfterCommitCallback(t *testing.T) {
	store := openDB(t)

	var called bool
	afterCommit := func(ctx context.Context) {
		t.Log(("After commit called"))
		called = true
	}

	_, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.AfterCommit(afterCommit)

		tx.Set(kvdb.Key("foo"), []byte("bar"))
		return nil, nil
	})
	if err != nil {
		t.Error(err)
	}
	if !called {
		t.Error("expect after commit callback to be called on commit")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAfterRollbackCallback(t *testing.T) {
	store := openDB(t)

	var called bool
	afterRollback := func(ctx context.Context) {
		called = true
	}

	_, err := store.Transact(context.Background(), func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.AfterRollback(afterRollback)
		return nil, errors.New("trigger a rollback")
	})
	if err == nil {
		t.Error("expect go get an error on rollback")
	}
	if !called {
		t.Error("expect after rollback callback to be called on rollback")
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func openDB(t *testing.T) *bbolt.Store {
	store, err := bbolt.Open(path.Join("db", t.Name()), 0600, "foo")
	if err != nil {
		t.Fatal(err)
	}
	return store
}
