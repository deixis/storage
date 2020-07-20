// Package badger is a `kvdb.Store` implementation for Badger.
// Badger is an embeddable, persistent, and fast key-value database.
// https://github.com/dgraph-io/badger#
package badger

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/deixis/errors"
	"github.com/deixis/pkg/unit"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/kvhook"
	badger "github.com/dgraph-io/badger/v2"
)

const Driver = "badger"

type Store struct {
	DB *badger.DB
}

// Open opens a Badger database and attempts to create it if
// it does not exist
func Open(path string, opts ...badger.Options) (*Store, error) {
	var o badger.Options
	if len(opts) > 0 {
		o = opts[0]
		o.Dir = path
	} else {
		o = badger.DefaultOptions(path)
		o.Logger = &nopLogger{}
	}

	db, err := badger.Open(o)
	if err != nil {
		return nil, errors.Wrap(err, "error opening badger")
	}
	return &Store{DB: db}, nil
}

func (s *Store) Transact(
	ctx context.Context,
	f func(kvdb.Transaction) (interface{}, error),
) (v interface{}, err error) {
	reg := kvhook.NewRegistry()

	err = s.DB.Update(func(tx *badger.Txn) error {
		t := &transaction{t: tx, context: ctx, registry: reg}
		v, err = f(t)
		for _, i := range t.iterators {
			i.Close()
		}
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
	err = s.DB.View(func(tx *badger.Txn) error {
		t := &transaction{t: tx, context: ctx}
		v, err = f(t)
		for _, i := range t.iterators {
			i.Close()
		}
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
	return &subspace{DB: s.DB, Prefix: p}, nil
}

func (s *Store) Close() error {
	return s.DB.Close()
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
	context   context.Context
	t         *badger.Txn
	errors    []error
	iterators []*badger.Iterator
	registry  kvhook.TransactionRegistry
}

func (t *transaction) Context() context.Context {
	return t.context
}

func (t *transaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	item, err := t.t.Get([]byte(key))
	switch err {
	case nil:
		return &futureByteSlice{item: item}
	case badger.ErrKeyNotFound:
		return &futureNopByteSlice{}
	default:
		return &futureErrorByteSlice{err: err}
	}
}

func (t *transaction) GetRange(r kvdb.KeyRange, options ...kvdb.RangeOption) kvdb.RangeResult {
	ro := RangeOptions{}
	for _, opt := range options {
		opt(&ro)
	}

	prefix := Prefix(r.Begin, r.End)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.Reverse = ro.Reverse
	if opts.PrefetchSize > ro.Limit {
		opts.PrefetchSize = ro.Limit
		opts.PrefetchValues = true // range iterator always loads values
	}

	begin := r.Begin
	end := r.End
	if opts.Reverse {
		begin, end = end, begin
	}

	var isInBound func(key []byte) bool
	if opts.Reverse {
		isInBound = func(key []byte) bool {
			return bytes.Compare(r.Begin, key) <= 0
		}
	} else {
		isInBound = func(key []byte) bool {
			return bytes.Compare(key, r.End) <= 0
		}
	}

	it := t.t.NewIterator(opts)
	it.Seek(begin)

	t.registerIterator(it)

	return &rangeResult{
		i: &rangeIterator{
			iter:      it,
			prefix:    opts.Prefix,
			end:       end,
			limit:     ro.Limit,
			isInBound: isInBound,
		},
	}
}

func (t *transaction) Set(key kvdb.Key, value []byte) {
	if unit.Byte(len(value)) > 10*unit.KB {
		t.errors = append(t.errors, fmt.Errorf("value too large (%d)", len(value)))
		return
	}

	err := t.t.Set(key, value)
	if err != nil {
		t.errors = append(t.errors, err)
	}
}

func (t *transaction) Clear(key kvdb.Key) {
	err := t.t.Delete(key)
	if err != nil {
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

func (c *transaction) registerIterator(i *badger.Iterator) {
	c.iterators = append(c.iterators, i)
}

type subspace struct {
	DB     *badger.DB
	Prefix []kvdb.TupleElement
}

func (s *subspace) Sub(el ...kvdb.TupleElement) kvdb.Subspace {
	return &subspace{
		DB:     s.DB,
		Prefix: append(s.Prefix, el...),
	}
}

func (s *subspace) Pack(t kvdb.Tuple) kvdb.Key {
	return packTuple(append(s.Prefix, t...))
}

func (s *subspace) Unpack(k kvdb.Key) (kvdb.Tuple, error) {
	return unpackTuple(bytes.TrimPrefix(k, packTuple(s.Prefix)))
}

type futureByteSlice struct {
	item *badger.Item
}

func (s futureByteSlice) Get() ([]byte, error) {
	return loadValue(s.item)
}

type futureNopByteSlice struct{}

func (s futureNopByteSlice) Get() ([]byte, error) {
	return nil, nil
}

type futureErrorByteSlice struct {
	err error
}

func (s futureErrorByteSlice) Get() ([]byte, error) {
	return nil, s.err
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
	iter      *badger.Iterator
	prefix    []byte
	end       []byte
	limit     int
	isInBound func(key []byte) bool

	scanned int
	done    bool
}

func (i *rangeIterator) Advance() bool {
	if i.done {
		return false
	}

	if i.scanned > 0 {
		i.iter.Next()
	}
	i.scanned++

	limitReached := i.limit != 0 && i.scanned > i.limit
	i.done = limitReached ||
		!i.iter.ValidForPrefix(i.prefix) ||
		!i.isInBound(i.iter.Item().Key())
	if i.done {
		i.iter.Close()
	}
	return !i.done
}

func (i *rangeIterator) Get() (kvdb.KeyValue, error) {
	if i.done {
		return kvdb.KeyValue{}, errors.New("end of iterator")
	}

	item := i.iter.Item()
	value, err := loadValue(item)
	if err != nil {
		return kvdb.KeyValue{}, err
	}

	return kvdb.KeyValue{
		Key:   loadKey(item),
		Value: value,
	}, nil
}

// buildTuple checks each tuple element type to make sure they are supported
// by FoundationDB and panic otherwise.
func buildTuple(v ...kvdb.TupleElement) kvdb.Tuple {
	var tup tuple.Tuple
	for i, elem := range v {
		switch elem := elem.(type) {
		case []byte:
			tup = append(tup, tuple.TupleElement(v))
		case string:
			tup = append(tup, tuple.TupleElement(v))
		case int, int8, int16, int32, int64:
			tup = append(tup, tuple.TupleElement(v))
		case float32, float64:
			tup = append(tup, tuple.TupleElement(v))
		case bool:
			tup = append(tup, tuple.TupleElement(v))
		case kvdb.Tuple:
			for _, e := range elem {
				tup = append(tup, tuple.TupleElement(e))
			}
		case []kvdb.TupleElement:
			for _, e := range elem {
				tup = append(tup, tuple.TupleElement(e))
			}
		case fdb.Key:
			tup = append(tup, tuple.TupleElement(v))
		case tuple.TupleElement:
			tup = append(tup, v)
		case tuple.Tuple:
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
	tup, err := tuple.Unpack(data)
	if err != nil {
		return nil, err
	}
	return unwrapTuple(tup), nil
}

func wrapTuple(t kvdb.Tuple) tuple.Tuple {
	tup := make([]tuple.TupleElement, len(t))
	for i, e := range t {
		tup[i] = tuple.TupleElement(e)
	}
	return tup
}

func unwrapTuple(t tuple.Tuple) kvdb.Tuple {
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

func loadKey(i *badger.Item) kvdb.Key {
	data := i.KeyCopy(nil)
	return kvdb.Key(data)
}

func loadValue(i *badger.Item) ([]byte, error) {
	data, err := i.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Prefix returns the common prefix between a and b
func Prefix(a, b kvdb.Key) (prefix kvdb.Key) {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if a[i] != b[i] {
			return prefix
		}

		prefix = append(prefix, a[i])
	}
	return prefix
}

type nopLogger struct{}

func (l *nopLogger) Errorf(string, ...interface{})   {}
func (l *nopLogger) Warningf(string, ...interface{}) {}
func (l *nopLogger) Infof(string, ...interface{})    {}
func (l *nopLogger) Debugf(string, ...interface{})   {}
