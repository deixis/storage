package foundationdb

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/deixis/errors"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/kvhook"
)

const Driver = "foundationdb"

type Store struct {
	FDB fdb.Database
}

func Open(clusterFile string) (kvdb.Store, error) {
	if err := fdb.APIVersion(200); err != nil {
		return nil, errors.Wrap(err, "incompatible fdb API version")
	}
	db, err := fdb.Open(clusterFile, []byte("DB"))
	if err != nil {
		return nil, errors.Wrap(err, "error opening DB")
	}
	_, err = db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		// A successful database open does not guarantee the connection works
		// so this transaction is just a connection check
		return nil, nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "error connecting to DB")
	}
	return &Store{FDB: db}, nil
}

func (s *Store) Transact(ctx context.Context, f func(kvdb.Transaction) (interface{}, error)) (interface{}, error) {
	reg := kvhook.NewRegistry()

	v, err := s.FDB.Transact(func(tx fdb.Transaction) (interface{}, error) {
		t := &transaction{t: tx, registry: reg}
		t.readTransaction.t = &tx
		t.readTransaction.context = ctx
		return f(t)
	})

	if err == nil {
		reg.FireEvent(ctx, kvhook.EventAfterCommit)
	} else {
		reg.FireEvent(ctx, kvhook.EventAfterRollback)
	}

	return v, err
}

func (s *Store) ReadTransact(ctx context.Context, f func(kvdb.ReadTransaction) (interface{}, error)) (interface{}, error) {
	return s.FDB.ReadTransact(func(tx fdb.ReadTransaction) (interface{}, error) {
		return f(&readTransaction{t: tx, context: ctx})
	})
}

func (s *Store) CreateOrOpenDir(path []string) (kvdb.DirectorySubspace, error) {
	ds, err := directory.CreateOrOpen(s.FDB, path, nil)
	if err != nil {
		return nil, err
	}
	return &ss{ss: ds}, nil
}

func (s *Store) Close() error {
	// Nothing to close
	return nil
}

func WithRangeStreamingMode(m fdb.StreamingMode) kvdb.RangeOption {
	return func(v interface{}) {
		o, ok := v.(*RangeOptions)
		if !ok {
			return
		}

		o.Mode = m
	}
}

type RangeOptions fdb.RangeOptions

func (o *RangeOptions) SetLimit(l int) {
	o.Limit = l
}

func (o *RangeOptions) SetReverse(r bool) {
	o.Reverse = r
}

type transaction struct {
	readTransaction

	t        fdb.Transaction
	registry kvhook.TransactionRegistry
}

func (t *transaction) Set(key kvdb.Key, value []byte) {
	t.t.Set(keyConvertible(key), value)
}

func (t *transaction) Clear(key kvdb.Key) {
	t.t.Clear(keyConvertible(key))
}

func (t *transaction) ClearRange(r kvdb.KeyRange) {
	kr := fdb.KeyRange{
		Begin: keyConvertible(r.Begin),
		End:   keyConvertible(r.End),
	}
	t.t.ClearRange(kr)
}

func (c *transaction) AfterCommit(f func(ctx context.Context)) {
	c.registry.AfterCommit(f)
}

func (c *transaction) AfterRollback(f func(ctx context.Context)) {
	c.registry.AfterRollback(f)
}

type readTransaction struct {
	t       fdb.ReadTransaction
	context context.Context
}

func (t *readTransaction) Context() context.Context {
	return t.context
}

func (t *readTransaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	return t.t.Get(keyConvertible(key))
}

func (t *readTransaction) GetRange(r kvdb.KeyRange, options ...kvdb.RangeOption) kvdb.RangeResult {
	kr := fdb.KeyRange{
		Begin: keyConvertible(r.Begin),
		End:   keyConvertible(r.End),
	}
	o := RangeOptions{}
	for _, opt := range options {
		opt(&o)
	}
	rr := t.t.GetRange(kr, fdb.RangeOptions(o))
	return &rangeResult{rr: rr}
}

type keyConvertible kvdb.Key

func (kc keyConvertible) FDBKey() fdb.Key {
	return fdb.Key(kc)
}

type keyRange kvdb.KeyRange

func (r *keyRange) FDBRangeKeySelectors() (begin, end fdb.Selectable) {
	begin = &keySelector{Key: r.Begin}
	end = &keySelector{Key: r.End}
	return begin, end
}

type keySelector struct {
	Key kvdb.Key
}

func (s *keySelector) FDBKeySelector() fdb.KeySelector {
	return fdb.KeySelector{
		Key: keyConvertible(s.Key),
	}
}

type rangeResult struct {
	rr fdb.RangeResult
}

func (r *rangeResult) GetSliceWithError() ([]kvdb.KeyValue, error) {
	res, err := r.rr.GetSliceWithError()
	if err != nil {
		return nil, err
	}
	s := make([]kvdb.KeyValue, len(res))
	for i, elem := range res {
		s[i] = kvdb.KeyValue{
			Key:   kvdb.Key(elem.Key),
			Value: elem.Value,
		}
	}
	return s, nil
}

func (r *rangeResult) Iterator() kvdb.RangeIterator {
	return &rangeIterator{ri: r.rr.Iterator()}
}

type rangeIterator struct {
	ri *fdb.RangeIterator
}

func (i *rangeIterator) Advance() bool {
	return i.ri.Advance()
}

func (i *rangeIterator) Get() (kvdb.KeyValue, error) {
	elem, err := i.ri.Get()
	return kvdb.KeyValue{
		Key:   kvdb.Key(elem.Key),
		Value: elem.Value,
	}, err
}

type ss struct {
	ss subspace.Subspace
}

func (s *ss) Sub(el ...kvdb.TupleElement) kvdb.Subspace {
	sub := s.ss.Sub(wrapTuple(kvdb.Tuple(el)))
	return &ss{ss: sub}
}

func (s *ss) Pack(t kvdb.Tuple) kvdb.Key {
	return kvdb.Key(s.ss.Pack(wrapTuple(t)))
}

func (s *ss) Unpack(k kvdb.Key) (kvdb.Tuple, error) {
	t, err := s.ss.Unpack(keyConvertible(k))
	if err != nil {
		return nil, err
	}
	return unwrapTuple(t), nil
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
