package kvdb

import "context"

type contextKey struct{}

var activeStoreKey = contextKey{}

// FromContext returns the storage instance associated with `ctx`
func FromContext(ctx context.Context) (Store, bool) {
	val := ctx.Value(activeStoreKey)
	if o, ok := val.(Store); ok {
		return o, true
	}
	return nil, false
}

// WithContext returns a copy of parent in which the storage is stored
func WithContext(ctx context.Context, o Store) context.Context {
	return context.WithValue(ctx, activeStoreKey, o)
}

var activeTxKey = contextKey{}

// TxFromContext returns a storage transaction instance associated with `ctx`
func TxFromContext(ctx context.Context) (Transaction, bool) {
	val := ctx.Value(activeTxKey)
	if o, ok := val.(Transaction); ok {
		return o, true
	}
	return nil, false
}

// TxWithContext returns a copy of parent in which a storage transaction is stored
func TxWithContext(ctx context.Context, tx Transaction) context.Context {
	return context.WithValue(ctx, activeTxKey, tx)
}

var activeRtxKey = contextKey{}

// RtxFromContext returns a storage read transaction instance associated with `ctx`
func RtxFromContext(ctx context.Context) (ReadTransaction, bool) {
	val := ctx.Value(activeRtxKey)
	if o, ok := val.(Transaction); ok {
		return o, true
	}
	return nil, false
}

// RtxFromContext returns a copy of parent in which a storage read transaction is stored
func RtxWithContext(ctx context.Context, tx ReadTransaction) context.Context {
	return context.WithValue(ctx, activeRtxKey, tx)
}
