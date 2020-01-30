package eventdb

import (
	"github.com/deixis/spine/net/pubsub"
	ep "github.com/deixis/storage/eventdb/pubsub"
)

type StoreOption interface {
	apply(*storeOptions)
}

// funcDialOption wraps a function that modifies storeOptions into an
// implementation of the StoreOption interface.
type funcStoreOption struct {
	f func(*storeOptions)
}

func (fdo *funcStoreOption) apply(do *storeOptions) {
	fdo.f(do)
}

func newFuncStoreOption(f func(*storeOptions)) *funcStoreOption {
	return &funcStoreOption{
		f: f,
	}
}

type storeOptions struct {
	pubSub      pubsub.PubSub
	drainPubSub bool
}

func defaultStoreOptions() *storeOptions {
	return &storeOptions{
		pubSub:      ep.New(),
		drainPubSub: true,
	}
}

func WithPubSub(ps pubsub.PubSub) StoreOption {
	return newFuncStoreOption(func(o *storeOptions) {
		o.pubSub = ps
		o.drainPubSub = false
	})
}
