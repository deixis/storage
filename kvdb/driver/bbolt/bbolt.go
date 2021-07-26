package bbolt

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/deixis/errors"
	"github.com/deixis/pkg/unit"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/kvhook"
	db "go.etcd.io/bbolt"
)

const Driver = "bbolt"

type Store struct {
	BB     *db.DB
	Bucket []byte
}

// Open opens a BBolt database and attempts to create the bucket if
// it does not exist
func Open(path string, mode os.FileMode, bucket string) (*Store, error) {
	bb, err := db.Open(path, mode, &db.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, errors.Wrap(err, "error opening bbolt")
	}
	return NewStore(bb, bucket)
}

// NewStore creates a store from BBolt and attempts to create the bucket if
// it does not exist
func NewStore(bb *db.DB, bucket string) (*Store, error) {
	err := bb.Update(func(tx *db.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return errors.Wrap(err, "error creating bucket")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Store{BB: bb, Bucket: []byte(bucket)}, nil
}

func (s *Store) Transact(
	ctx context.Context,
	f func(kvdb.Transaction) (interface{}, error),
) (v interface{}, err error) {
	reg := kvhook.NewRegistry()

	err = s.BB.Update(func(tx *db.Tx) error {
		b := tx.Bucket(s.Bucket)
		t := &transaction{t: tx, b: b, context: ctx, registry: reg}
		v, err = f(t)
		if err != nil {
			return err
		}
		if len(t.errors) > 0 {
			return errs(t.errors)
		}
		return nil
	})

	if err == nil {
		reg.FireEvent(ctx, kvhook.EventAfterCommit)
	} else {
		reg.FireEvent(ctx, kvhook.EventAfterRollback)
	}

	return v, err
}

func (s *Store) ReadTransact(
	ctx context.Context,
	f func(kvdb.ReadTransaction) (interface{}, error),
) (v interface{}, err error) {
	err = s.BB.View(func(tx *db.Tx) error {
		b := tx.Bucket(s.Bucket)
		t := &transaction{t: tx, b: b, context: ctx}
		v, err = f(t)
		if err != nil {
			return err
		}
		if len(t.errors) > 0 {
			return errs(t.errors)
		}
		return nil
	})
	return v, err
}

func (s *Store) CreateOrOpenDir(path []string) (kvdb.DirectorySubspace, error) {
	p := make([]kvdb.TupleElement, len(path))
	for i, elem := range path {
		p[i] = elem
	}
	buildTuple(p...) // Just ensure everything is valid
	return &subspace{BB: s.BB, Prefix: p}, nil
}

func (s *Store) Close() error {
	return s.BB.Close()
}

type RangeOptions struct {
	Limit   int
	Reverse bool
}

func (o *RangeOptions) SetLimit(l int) {
	o.Limit = l
}

func (o *RangeOptions) SetReverse(r bool) {
	o.Reverse = r
}

type transaction struct {
	context  context.Context
	t        *db.Tx
	b        *db.Bucket
	errors   []error
	registry kvhook.TransactionRegistry
}

func (t *transaction) Context() context.Context {
	return t.context
}

func (t *transaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	return futureByteSlice(t.b.Get([]byte(key)))
}

func (t *transaction) GetRange(r kvdb.KeyRange, options ...kvdb.RangeOption) kvdb.RangeResult {
	o := RangeOptions{}
	for _, opt := range options {
		opt(&o)
	}

	var i kvdb.RangeIterator
	if o.Reverse {
		i = &reverseRangeIterator{
			begin: r.Begin,
			end:   r.End,
			limit: o.Limit,
			c:     t.b.Cursor(),
		}
	} else {
		i = &rangeIterator{
			begin: r.Begin,
			end:   r.End,
			limit: o.Limit,
			c:     t.b.Cursor(),
		}
	}
	return &rangeResult{
		i: i,
	}
}

func (t *transaction) Set(key kvdb.Key, value []byte) {
	if unit.Byte(len(value)) > 10*unit.KB {
		t.errors = append(t.errors, fmt.Errorf("value too large (%d)", len(value)))
		return
	}

	err := t.b.Put(key, value)
	if err != nil {
		// occur if :
		// - if the bucket was created from a read-only transaction
		// - if the key is blank
		// - if the key is too large
		// - if the value is too large.
		t.errors = append(t.errors, err)
	}
}

func (t *transaction) Clear(key kvdb.Key) {
	err := t.b.Delete(key)
	if err != nil {
		// occur if :
		// - if the bucket was created from a read-only transaction.
		t.errors = append(t.errors, err)
	}
}

func (t *transaction) ClearRange(r kvdb.KeyRange) {
	i := t.GetRange(r).Iterator()
	for i.Advance() {
		kv, err := i.Get()
		if err != nil {
			t.errors = append(t.errors, err)
			continue
		}
		t.Clear(kv.Key)
	}
}

func (c *transaction) AfterCommit(f func(ctx context.Context)) {
	c.registry.AfterCommit(f)
}

func (c *transaction) AfterRollback(f func(ctx context.Context)) {
	c.registry.AfterRollback(f)
}

type subspace struct {
	BB     *db.DB
	Prefix []kvdb.TupleElement
}

func (s *subspace) Sub(el ...kvdb.TupleElement) kvdb.Subspace {
	return &subspace{
		BB:     s.BB,
		Prefix: append(s.Prefix, el...),
	}
}

func (s *subspace) Pack(t kvdb.Tuple) kvdb.Key {
	return packTuple(append(s.Prefix, t...))
}

func (s *subspace) Unpack(k kvdb.Key) (kvdb.Tuple, error) {
	return unpackTuple(bytes.TrimPrefix(k, packTuple(s.Prefix)))
}

type futureByteSlice []byte

func (s futureByteSlice) Get() ([]byte, error) {
	return s, nil
}

type rangeResult struct {
	i kvdb.RangeIterator
}

func (r *rangeResult) GetSliceWithError() ([]kvdb.KeyValue, error) {
	i := r.Iterator()
	var res []kvdb.KeyValue
	for i.Advance() {
		elem, err := i.Get()
		if err != nil {
			return nil, err
		}
		res = append(res, elem)
	}
	return res, nil
}

func (r *rangeResult) Iterator() kvdb.RangeIterator {
	return r.i
}

type rangeIterator struct {
	started bool
	begin   []byte
	end     []byte
	limit   int

	c    *db.Cursor
	keys int
	done bool

	k []byte
	v []byte
}

func (i *rangeIterator) Advance() bool {
	if i.done {
		return false
	}
	if i.limit != 0 && i.keys == i.limit {
		return false
	}
	i.keys++

	if !i.started {
		i.started = true
		i.k, i.v = i.c.Seek(i.begin)
	} else {
		i.k, i.v = i.c.Next()
	}
	cont := len(i.k) > 0 && bytes.Compare(i.k, i.end) < 0
	if !cont {
		i.k = nil
		i.v = nil
		i.done = true
	}
	return cont
}

func (i *rangeIterator) Get() (kvdb.KeyValue, error) {
	if i.done {
		return kvdb.KeyValue{}, errors.New("end of iterator")
	}
	return kvdb.KeyValue{
		Key:   kvdb.Key(i.k),
		Value: i.v,
	}, nil
}

type reverseRangeIterator struct {
	started bool
	begin   []byte
	end     []byte
	limit   int

	c    *db.Cursor
	keys int
	done bool

	k []byte
	v []byte
}

func (i *reverseRangeIterator) Advance() bool {
	if i.done {
		return false
	}
	if i.limit != 0 && i.keys == i.limit {
		return false
	}
	i.keys++

	if !i.started {
		i.started = true
		i.k, i.v = i.c.Seek(i.end)

		// BBolt goes to the next key if it doesn't get an exact match. Because the
		// iterator is going backward, we need to find the first matching key.
		if len(i.k) == 0 {
			// When there are no available keys after the upper bound, Bbolt assume
			// there are no keys to iterate over.
			i.k, i.v = i.c.Prev()
		}
		for bytes.Compare(i.k, i.end) > 0 && bytes.Compare(i.k, i.begin) >= 0 {
			// Rewind (it should not happen more than once)
			i.k, i.v = i.c.Prev()
		}
	} else {
		i.k, i.v = i.c.Prev()
	}
	cont := len(i.k) > 0 && bytes.Compare(i.k, i.begin) >= 0
	if !cont {
		i.k = nil
		i.v = nil
		i.done = true
	}
	return cont
}

func (i *reverseRangeIterator) Get() (kvdb.KeyValue, error) {
	if i.done {
		return kvdb.KeyValue{}, errors.New("end of iterator")
	}
	return kvdb.KeyValue{
		Key:   kvdb.Key(i.k),
		Value: i.v,
	}, nil
}

// buildTuple checks each tuple element type to make sure they are supported
// by FoundationDB and panic otherwise.
func buildTuple(v ...kvdb.TupleElement) kvdb.Tuple {
	var tup kvdb.Tuple
	for i, elem := range v {
		switch elem := elem.(type) {
		case []byte:
			tup = append(tup, kvdb.TupleElement(v))
		case string:
			tup = append(tup, kvdb.TupleElement(v))
		case int, int8, int16, int32, int64:
			tup = append(tup, kvdb.TupleElement(v))
		case float32, float64:
			tup = append(tup, kvdb.TupleElement(v))
		case bool:
			tup = append(tup, kvdb.TupleElement(v))
		case kvdb.Tuple:
			for _, e := range elem {
				tup = append(tup, kvdb.TupleElement(e))
			}
		case []kvdb.TupleElement:
			for _, e := range elem {
				tup = append(tup, kvdb.TupleElement(e))
			}
		case kvdb.TupleElement:
			tup = append(tup, v)
		case nil:
			tup = append(tup, kvdb.TupleElement(v))
		default:
			panic(fmt.Sprintf("unsupported tuple element %d - type %T (%v)", i, v, v))
		}
	}
	return unwrapTuple(tup)
}

func packTuple(t kvdb.Tuple) []byte {
	tup := wrapTuple(t)
	return tup.Pack()
}

func unpackTuple(data []byte) (kvdb.Tuple, error) {
	tup, err := kvdb.Unpack(data)
	if err != nil {
		return nil, err
	}
	return unwrapTuple(tup), nil
}

func wrapTuple(t kvdb.Tuple) kvdb.Tuple {
	tup := make([]kvdb.TupleElement, len(t))
	for i, e := range t {
		tup[i] = kvdb.TupleElement(e)
	}
	return tup
}

func unwrapTuple(t kvdb.Tuple) kvdb.Tuple {
	tup := make([]kvdb.TupleElement, len(t))
	for i, e := range t {
		tup[i] = kvdb.TupleElement(e)
	}
	return tup
}

type errs []error

func (e errs) Error() string {
	var b bytes.Buffer
	for _, err := range e {
		b.WriteString(err.Error())
		b.WriteString(", ")
	}
	return b.String()
}
