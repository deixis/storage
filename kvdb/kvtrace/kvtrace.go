package kvtrace

import (
	"context"

	"github.com/deixis/spine/tracing"
	"github.com/deixis/storage/kvdb"
	opentracing "github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

// store implements instrument for tracing kv store requests
type store struct {
	s kvdb.Store
}

// Trace wraps Store s with tracing instruments
func Trace(s kvdb.Store) kvdb.Store {
	return &store{s: s}
}

func (c *store) Transact(
	ctx context.Context,
	f func(kvdb.Transaction) (interface{}, error),
) (v interface{}, err error) {
	var span opentracing.Span
	span, ctx = tracing.StartSpanFromContext(ctx, "storage.kvdb.tx")
	defer span.Finish()
	span.LogFields(
		olog.String("type", "storage.kv"),
	)

	return c.s.Transact(ctx, func(t kvdb.Transaction) (interface{}, error) {
		return f(&transaction{t: t, ctx: ctx})
	})
}

func (c *store) ReadTransact(
	ctx context.Context,
	f func(kvdb.ReadTransaction) (interface{}, error),
) (interface{}, error) {
	var span opentracing.Span
	span, ctx = tracing.StartSpanFromContext(ctx, "storage.kvdb.readTx")
	defer span.Finish()
	span.LogFields(
		olog.String("type", "storage.kv"),
	)

	return c.s.ReadTransact(ctx, func(t kvdb.ReadTransaction) (interface{}, error) {
		return f(&readTransaction{t: t, ctx: ctx})
	})
}

func (c *store) CreateOrOpenDir(path []string) (kvdb.DirectorySubspace, error) {
	return c.s.CreateOrOpenDir(path)
}

func (c *store) Close() error {
	return c.s.Close()
}

type readTransaction struct {
	t   kvdb.ReadTransaction
	ctx context.Context
}

func (t *readTransaction) Context() context.Context {
	return t.t.Context()
}

func (c *readTransaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.get")
	defer span.Finish()
	span.LogFields(
		olog.String("key", string(key)),
	)

	return &futureByteSlice{
		s: c.t.Get(key),
	}
}

func (c *readTransaction) GetRange(
	r kvdb.KeyRange, options ...kvdb.RangeOption,
) kvdb.RangeResult {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.getRange")
	defer span.Finish()
	span.LogFields(
		olog.String("begin", string(r.Begin)),
		olog.String("end", string(r.End)),
	)

	return &rangeResult{
		rr: c.t.GetRange(r, options...),
	}
}

type transaction struct {
	t   kvdb.Transaction
	ctx context.Context
}

// EncryptTx wraps Transaction t with an encryption layer
func EncryptTx(t kvdb.Transaction) kvdb.Transaction {
	return &transaction{t: t}
}

func (t *transaction) Context() context.Context {
	return t.t.Context()
}

func (c *transaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.get")
	defer span.Finish()
	span.LogFields(
		olog.String("key", string(key)),
	)

	return &futureByteSlice{
		s: c.t.Get(key),
	}
}

func (c *transaction) GetRange(
	r kvdb.KeyRange, options ...kvdb.RangeOption,
) kvdb.RangeResult {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.getRange")
	defer span.Finish()
	span.LogFields(
		olog.String("begin", string(r.Begin)),
		olog.String("end", string(r.End)),
	)

	return &rangeResult{
		rr: c.t.GetRange(r, options...),
	}
}

func (c *transaction) Set(key kvdb.Key, value []byte) {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.set")
	defer span.Finish()
	span.LogFields(
		olog.String("key", string(key)),
	)

	c.t.Set(key, value)
}

func (c *transaction) Clear(key kvdb.Key) {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.clear")
	defer span.Finish()
	span.LogFields(
		olog.String("key", string(key)),
	)

	c.t.Clear(key)
}

func (c *transaction) ClearRange(r kvdb.KeyRange) {
	span, _ := tracing.StartSpanFromContext(c.ctx, "storage.kvdb.clearRange")
	defer span.Finish()
	span.LogFields(
		olog.String("begin", string(r.Begin)),
		olog.String("end", string(r.End)),
	)

	c.t.ClearRange(r)
}

func (c *transaction) AfterCommit(f func(ctx context.Context)) {
	c.t.AfterCommit(f)
}

func (c *transaction) AfterRollback(f func(ctx context.Context)) {
	c.t.AfterRollback(f)
}

type futureByteSlice struct {
	s kvdb.FutureByteSlice
}

func (c *futureByteSlice) Get() ([]byte, error) {
	return c.s.Get()
}

type rangeResult struct {
	rr kvdb.RangeResult
}

func (c *rangeResult) GetSliceWithError() ([]kvdb.KeyValue, error) {
	return c.rr.GetSliceWithError()
}

func (c *rangeResult) Iterator() kvdb.RangeIterator {
	return &rangeIterator{
		ri: c.rr.Iterator(),
	}
}

type rangeIterator struct {
	ri kvdb.RangeIterator
}

func (c *rangeIterator) Advance() bool {
	return c.ri.Advance()
}

func (c *rangeIterator) Get() (kv kvdb.KeyValue, err error) {
	return c.ri.Get()
}
