package kvcrypto

import (
	"context"

	"github.com/deixis/spine/crypto"
	"github.com/deixis/storage/kvdb"
)

type store struct {
	s kvdb.Store
	r *crypto.Rotor
}

// Encrypt wraps Store s with an encryption layer
func Encrypt(s kvdb.Store, r *crypto.Rotor) kvdb.Store {
	return &store{s: s, r: r}
}

func (c *store) Transact(
	ctx context.Context,
	f func(kvdb.Transaction) (interface{}, error),
) (v interface{}, err error) {
	return c.s.Transact(ctx, func(t kvdb.Transaction) (interface{}, error) {
		return f(&transaction{t: t, r: c.r})
	})
}

func (c *store) ReadTransact(
	ctx context.Context,
	f func(kvdb.ReadTransaction) (interface{}, error),
) (interface{}, error) {
	return c.s.ReadTransact(ctx, func(t kvdb.ReadTransaction) (interface{}, error) {
		return f(&readTransaction{t: t, r: c.r})
	})
}

func (c *store) CreateOrOpenDir(path []string) (kvdb.DirectorySubspace, error) {
	return c.s.CreateOrOpenDir(path)
}

func (c *store) Close() error {
	return c.s.Close()
}

type readTransaction struct {
	t kvdb.ReadTransaction
	r *crypto.Rotor
}

// EncryptReadTx wraps ReadTransaction t with an encryption layer
func EncryptReadTx(
	t kvdb.ReadTransaction, r *crypto.Rotor,
) kvdb.ReadTransaction {
	return &readTransaction{t: t, r: r}
}

func (t *readTransaction) Context() context.Context {
	return t.t.Context()
}

func (c *readTransaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	return &futureByteSlice{
		s: c.t.Get(key),
		r: c.r,
	}
}

func (c *readTransaction) GetRange(
	r kvdb.KeyRange, options ...kvdb.RangeOption,
) kvdb.RangeResult {
	return &rangeResult{
		rr: c.t.GetRange(r, options...),
		r:  c.r,
	}
}

type transaction struct {
	t kvdb.Transaction
	r *crypto.Rotor
}

// EncryptTx wraps Transaction t with an encryption layer
func EncryptTx(t kvdb.Transaction, r *crypto.Rotor) kvdb.Transaction {
	return &transaction{t: t, r: r}
}

func (t *transaction) Context() context.Context {
	return t.t.Context()
}

func (c *transaction) Get(key kvdb.Key) kvdb.FutureByteSlice {
	return &futureByteSlice{
		s: c.t.Get(key),
		r: c.r,
	}
}

func (c *transaction) GetRange(
	r kvdb.KeyRange, options ...kvdb.RangeOption,
) kvdb.RangeResult {
	return &rangeResult{
		rr: c.t.GetRange(r, options...),
		r:  c.r,
	}
}

func (c *transaction) Set(key kvdb.Key, value []byte) {
	if len(value) == 0 {
		c.t.Set(key, value)
		return
	}

	encrypted, err := c.r.Encrypt(value)
	if err != nil {
		panic(err)
		// TODO: Should we change the contract and return an error here?
	}
	c.t.Set(key, encrypted)
}

func (c *transaction) Clear(key kvdb.Key) {
	c.t.Clear(key)
}

func (c *transaction) ClearRange(r kvdb.KeyRange) {
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
	r *crypto.Rotor
}

func (c *futureByteSlice) Get() ([]byte, error) {
	crypto, err := c.s.Get()
	if err != nil {
		return nil, err
	}
	if len(crypto) == 0 {
		return crypto, nil
	}

	return c.r.Decrypt(crypto)
}

type rangeResult struct {
	rr kvdb.RangeResult
	r  *crypto.Rotor
}

func (c *rangeResult) GetSliceWithError() ([]kvdb.KeyValue, error) {
	s, err := c.rr.GetSliceWithError()
	if err != nil {
		return nil, err
	}

	for i := range s {
		if len(s[i].Value) == 0 {
			continue
		}

		plain, err := c.r.Decrypt(s[i].Value)
		if err != nil {
			return nil, err
		}
		s[i].Value = plain
	}
	return s, nil
}

func (c *rangeResult) Iterator() kvdb.RangeIterator {
	return &rangeIterator{
		ri: c.rr.Iterator(),
		r:  c.r,
	}
}

type rangeIterator struct {
	ri kvdb.RangeIterator
	r  *crypto.Rotor
}

func (c *rangeIterator) Advance() bool {
	return c.ri.Advance()
}

func (c *rangeIterator) Get() (kv kvdb.KeyValue, err error) {
	kv, err = c.ri.Get()
	if err != nil || len(kv.Value) == 0 {
		return kv, err
	}

	plain, err := c.r.Decrypt(kv.Value)
	if err != nil {
		return kv, err
	}
	kv.Value = plain
	return kv, err
}
