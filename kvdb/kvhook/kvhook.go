package kvhook

import (
	"context"
)

// Hook is a simple function
type Hook func(ctx context.Context)

// Event represents a transaction lifecycle event
type Event int

const (
	EventAfterCommit Event = iota
	EventAfterRollback
)

// TransactionHook allows to hook into the life cycle of a transaction.
type TransactionHook interface {
	// AfterCommit is called after a transaction commit (success)
	AfterCommit(f func(ctx context.Context))
	// AfterRollback is called after a transaction rollback (failure)
	AfterRollback(f func(ctx context.Context))
}

// TransactionRegistry is a transaction callback registry that can be used
// by all drivers.
type TransactionRegistry interface {
	TransactionHook

	// FireEvent calls all callbacks registered to event `e` sequentially
	FireEvent(ctx context.Context, e Event)
}

type registry struct {
	// Hooks contains all callbacks registered
	//
	// Note: Map is not thread safe, but we can assume for now that
	// they will be registered within the same thread. Transactions are usually
	// not shared accross multiple threads.
	Hooks map[Event][]Hook
}

func NewRegistry() TransactionRegistry {
	return &registry{
		Hooks: make(map[Event][]Hook),
	}
}

func (r *registry) AfterCommit(f func(ctx context.Context)) {
	r.Hooks[EventAfterCommit] = append(r.Hooks[EventAfterCommit], f)
}

func (r *registry) AfterRollback(f func(ctx context.Context)) {
	r.Hooks[EventAfterRollback] = append(r.Hooks[EventAfterRollback], f)
}

func (r *registry) FireEvent(ctx context.Context, e Event) {
	for _, c := range r.Hooks[e] {
		c(ctx)
	}
}
