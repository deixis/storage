package kvdb

import (
	"context"

	"github.com/deixis/storage/kvdb/kvhook"
)

type Store interface {
	Transact(ctx context.Context, f func(Transaction) (interface{}, error)) (interface{}, error)
	ReadTransact(ctx context.Context, f func(ReadTransaction) (interface{}, error)) (interface{}, error)

	CreateOrOpenDir(path []string) (DirectorySubspace, error)
	Close() error
}

// ReadTransaction is a read-only transaction
type ReadTransaction interface {
	Context() context.Context

	Get(key Key) FutureByteSlice
	GetRange(r KeyRange, options ...RangeOption) RangeResult
}

// WriteTransaction is a read/write transaction
type WriteTransaction interface {
	Set(key Key, value []byte)
	Clear(key Key)
	ClearRange(r KeyRange)
}

// Transaction is a read-write transaction
type Transaction interface {
	ReadTransaction
	WriteTransaction

	kvhook.TransactionHook
}

// DirectorySubspace represents a Directory that may also be used as a Subspace
// to store key/value pairs. Subdirectories of a root directory (as returned by
// Root or NewDirectoryLayer) are DirectorySubspaces, and provide all methods of
// the Directory and subspace.Subspace interfaces.
type DirectorySubspace interface {
	Subspace
}

// Subspace represents a well-defined region of keyspace in a database.
type Subspace interface {
	// Sub returns a new Subspace whose prefix extends this Subspace with the
	// encoding of the provided element(s). If any of the elements are not a
	// valid tuple.TupleElement, Sub will panic.
	Sub(el ...TupleElement) Subspace

	// Pack returns the key encoding the specified Tuple with the prefix of this
	// Subspace prepended.
	Pack(t Tuple) Key

	// Unpack returns the Tuple encoded by the given key with the prefix of this
	// Subspace removed. Unpack will return an error if the key is not in this
	// Subspace or does not encode a well-formed Tuple.
	Unpack(k Key) (Tuple, error)
}

type FutureByteSlice interface {
	// Get returns a database value (or nil if there is no value), or an error
	// if the asynchronous operation associated with this future did not
	// successfully complete. The current goroutine will be blocked until the
	// future is ready.
	Get() ([]byte, error)
}

// KeyRange is an ExactRange constructed from a pair of KeyConvertibles. Note
// that the default zero-value of KeyRange specifies an empty range before all
// keys in the database.
type KeyRange struct {
	Begin, End Key
}

// RangeOption is a range option that is driver-specific
type RangeOption func(v interface{})

type rangeLimitOption interface {
	SetLimit(l int)
}

func WithRangeLimit(l int) RangeOption {
	return func(v interface{}) {
		o, ok := v.(rangeLimitOption)
		if !ok {
			return
		}
		o.SetLimit(l)
	}
}

type rangeReverseOption interface {
	SetReverse(r bool)
}

func WithRangeReverse(r bool) RangeOption {
	return func(v interface{}) {
		o, ok := v.(rangeReverseOption)
		if !ok {
			return
		}
		o.SetReverse(r)
	}
}

type RangeResult interface {
	GetSliceWithError() ([]KeyValue, error)
	Iterator() RangeIterator
}

type RangeIterator interface {
	Advance() bool
	Get() (kv KeyValue, e error)
}

// KeyValue represents a single key-value pair in the database.
type KeyValue struct {
	Key   Key
	Value []byte
}

// Key represents a K/V store key, a lexicographically-ordered sequence of
// bytes.
type Key []byte
