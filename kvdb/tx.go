package kvdb

import (
	"context"
	"errors"
	"time"
)

const (
	// MaxTransactionDuration gives longest possible transaction. This limit corresponds
	// to FoundationDB transaction limit.
	// More info: https://apple.github.io/foundationdb/known-limitations.html
	MaxTransactionDuration = 5 * time.Second
)

var (
	errRollback = errors.New("transaction rollback")
)

// ReadTransactionUnit opens a database transaction block in the background and
// allows to execute ad-hoc database operations.
// Commit or Rollback have to be called explicitely.
type ReadTransactionUnit struct {
	deadline time.Time
	command  chan func(ReadTransaction) error
	commit   chan bool
	result   chan error
}

// OpenReadTransaction opens a read database transaction
func OpenReadTransaction(
	ctx context.Context,
	fn func(
		ctx context.Context,
		f func(ReadTransaction) (interface{}, error),
	) (interface{}, error),
) *ReadTransactionUnit {
	t := &ReadTransactionUnit{
		deadline: time.Now().Add(MaxTransactionDuration),
		command:  make(chan func(ReadTransaction) error),
		commit:   make(chan bool),
		result:   make(chan error, 1),
	}

	go func() {
		_, err := fn(ctx, func(tx ReadTransaction) (interface{}, error) {
			// TODO: Support for retry?
			for {
				select {
				case cmd := <-t.command:
					if err := cmd(tx); err != nil {
						return nil, err
					}
				case commit := <-t.commit:
					if commit {
						return nil, nil
					}
					return nil, errRollback
				case <-time.Tick(t.deadline.Sub(time.Now())):
					return nil, errors.New("transaction running for too long")
				}
			}
		})
		t.result <- err
	}()
	return t
}

func (tx *ReadTransactionUnit) Context() context.Context {
	var ctx context.Context
	fn := func(tx ReadTransaction) error {
		ctx = tx.Context()
		return nil
	}
	tx.command <- fn
	return ctx
}

func (tx *ReadTransactionUnit) Get(key Key) FutureByteSlice {
	var s FutureByteSlice
	fn := func(tx ReadTransaction) error {
		s = tx.Get(key)
		return nil
	}
	tx.command <- fn
	return s
}

func (tx *ReadTransactionUnit) GetRange(r KeyRange, options ...RangeOption) RangeResult {
	var rr RangeResult
	fn := func(tx ReadTransaction) error {
		rr = tx.GetRange(r, options...)
		return nil
	}
	tx.command <- fn
	return rr
}

func (tx *ReadTransactionUnit) Commit() error {
	tx.commit <- true
	return <-tx.result
}

func (tx *ReadTransactionUnit) Rollback() error {
	tx.commit <- false
	err := <-tx.result
	if err == errRollback {
		return nil
	}
	return err
}

// TransactionUnit opens a database transaction block in the background and
// allows to execute ad-hoc database operations.
// Commit or Rollback have to be called explicitely.
type TransactionUnit struct {
	deadline time.Time
	command  chan func(Transaction) error
	commit   chan bool
	result   chan error
}

// OpenTransaction opens a read/write database transaction
func OpenTransaction(
	ctx context.Context,
	fn func(
		ctx context.Context,
		f func(Transaction) (interface{}, error),
	) (interface{}, error),
) *TransactionUnit {
	t := &TransactionUnit{
		deadline: time.Now().Add(MaxTransactionDuration),
		command:  make(chan func(Transaction) error),
		commit:   make(chan bool),
		result:   make(chan error, 1),
	}

	go func() {
		_, err := fn(ctx, func(tx Transaction) (interface{}, error) {
			// TODO: Support for retry?
			for {
				select {
				case cmd := <-t.command:
					if err := cmd(tx); err != nil {
						return nil, err
					}
				case commit := <-t.commit:
					if commit {
						return nil, nil
					}
					return nil, errRollback
				case <-time.Tick(t.deadline.Sub(time.Now())):
					return nil, errors.New("transaction running for too long")
				}
			}
		})
		t.result <- err
	}()
	return t
}

func (tx *TransactionUnit) Context() context.Context {
	var ctx context.Context
	fn := func(tx Transaction) error {
		ctx = tx.Context()
		return nil
	}
	tx.command <- fn
	return ctx
}

func (tx *TransactionUnit) Get(key Key) FutureByteSlice {
	var s FutureByteSlice
	fn := func(tx Transaction) error {
		s = tx.Get(key)
		return nil
	}
	tx.command <- fn
	return s
}

func (tx *TransactionUnit) GetRange(r KeyRange, options ...RangeOption) RangeResult {
	var rr RangeResult
	fn := func(tx Transaction) error {
		rr = tx.GetRange(r, options...)
		return nil
	}
	tx.command <- fn
	return rr
}

func (tx *TransactionUnit) Set(key Key, value []byte) {
	fn := func(tx Transaction) error {
		tx.Set(key, value)
		return nil
	}
	tx.command <- fn
}

func (tx *TransactionUnit) Clear(key Key) {
	fn := func(tx Transaction) error {
		tx.Clear(key)
		return nil
	}
	tx.command <- fn
}

func (tx *TransactionUnit) ClearRange(r KeyRange) {
	fn := func(tx Transaction) error {
		tx.ClearRange(r)
		return nil
	}
	tx.command <- fn
}

func (tx *TransactionUnit) Commit() {
	tx.commit <- true
}

func (tx *TransactionUnit) Rollback() {
	tx.commit <- false
}
