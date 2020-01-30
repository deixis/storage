package pubsub

import (
	"context"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/deixis/spine/bg"
	lcontext "github.com/deixis/spine/context"
	"github.com/deixis/spine/log"
	"github.com/deixis/spine/net"
	"github.com/deixis/spine/net/pubsub"
	"github.com/deixis/spine/tracing"
)

const (
	defaultBuffer = 50
)

type Inmem struct {
	mu    sync.RWMutex
	state uint32

	rootctx context.Context
	reg     *bg.Reg

	channels  map[string]*groupMap
	inFlights int64
	empty     *sync.Cond
}

func New() pubsub.PubSub {
	ps := &Inmem{
		rootctx:   context.Background(),
		channels:  map[string]*groupMap{},
		inFlights: 0,
		empty:     sync.NewCond(&sync.Mutex{}),
		reg:       newRegistry(),
	}
	atomic.StoreUint32(&ps.state, net.StateUp)
	return ps
}

func (ps *Inmem) Start(ctx context.Context) error {
	ps.rootctx = ctx
	return nil
}

func (ps *Inmem) Publish(
	ctx context.Context,
	ch string,
	data []byte,
) error {
	if !ps.isState(net.StateUp) {
		log.Warn(ps.rootctx, "pubsub.pub.draining", "PubSub is down or draining")
		return net.ErrDraining
	}

	// Load channel groups
	ps.mu.RLock()
	groups, ok := ps.channels[ch]
	ps.mu.RUnlock()

	// Discard message in case nobody subscribed to that channel
	if !ok {
		log.Trace(ps.rootctx, "pubsub.pub.drop", "No subscribers to that channel",
			log.String("channel", ch),
		)
		return nil
	}

	for _, group := range *groups {
		// Fan out message for each group
		// Currently relying on Go map randomisation algorithm. Could be improved
		// with a proper round robin algorithm (or other based on usage)
		group.Subs[0] <- &message{
			Transit: lcontext.TransitFromContext(ctx),
			Data:    data,
		}
		v := atomic.AddInt64(&ps.inFlights, 1)
		if v == 1 {
			ps.empty.L.Lock()
		}
	}
	return nil
}

func (ps *Inmem) Subscribe(q, ch string, h pubsub.MsgHandler) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Create channel if it does not exist and dispatch worker
	groups, ok := ps.channels[ch]
	if !ok {
		groups = &groupMap{}
		ps.channels[ch] = groups

		// Dispatch worker for that channel
		// New subscribes to the channel won't be updated on the worker
		// Normally they should all have been subscribed before the first load
		log.Trace(ps.rootctx, "pubsub.dispatch", "Dispatch worker",
			log.String("channel", ch),
		)
		ps.reg.Dispatch(&worker{
			rootctx: ps.rootctx,
			stop:    make(chan bool),
			loadGroups: func() groupMap {
				ps.mu.RLock()
				defer ps.mu.RUnlock()
				return *ps.channels[ch]
			},
			msgProcessed: func() {
				inFlights := atomic.AddInt64(&ps.inFlights, -1)
				if inFlights <= 0 {
					ps.empty.Broadcast()
				}
			},
		})
	}

	// Create group if it does not exist
	g, ok := (*groups)[q]
	if !ok {
		g = &group{}
		(*groups)[q] = g
	}

	g.AddHandler(h, defaultBuffer)
	return nil
}

func (ps *Inmem) Drain() {
	// Stop accepting new messages
	atomic.StoreUint32(&ps.state, net.StateDrain)

	// Wait for all in-flight messages to be delivered
	inFlights := atomic.LoadInt64(&ps.inFlights)
	if inFlights > 0 {
		log.Trace(ps.rootctx, "eventdb.pubsub.draining", "Wait for in-flight messages to be processed",
			log.Int64("inFlights", inFlights),
		)
		// FIXME: There can be a race condition here.
		ps.empty.Wait()
	}

	// Drain registry
	ps.reg.Drain()

	// Cleanup
	ps.mu.Lock()
	ps.channels = map[string]*groupMap{}
	ps.reg = newRegistry()
	ps.mu.Unlock()
	atomic.StoreUint32(&ps.state, net.StateDown)

	log.Trace(ps.rootctx, "eventdb.pubsub.drained", "PubSub drained")
}

func (ps *Inmem) Close() error {
	atomic.StoreUint32(&ps.state, net.StateDown)
	return nil
}

// isState checks the current server state
func (ps *Inmem) isState(state uint32) bool {
	return atomic.LoadUint32(&ps.state) == uint32(state)
}

type worker struct {
	rootctx context.Context
	stop    chan bool

	loadGroups   func() groupMap
	msgProcessed func()
}

func (w *worker) Start() {
	for {
		// A channel is created when at least one group subscribes, so we can
		// assume this for loop will never be skipped
		groups := w.loadGroups()
		for _, group := range groups {
			// Pick first sub for now (randomise later)
			sub := group.Subs[0]
			handler := group.Handlers[0]

			select {
			case <-w.stop:
				// Close worker
				return
			case msg := <-sub:
				w.deliver(msg, handler)
			}
		}
	}
}

func (w *worker) deliver(msg *message, handler pubsub.MsgHandler) {
	// Create request root context
	ctx, cancel := context.WithCancel(w.rootctx)
	defer cancel()

	// Pick transit or create a new one, and attach it to context
	if msg.Transit != nil {
		ctx = lcontext.TransitWithContext(ctx, msg.Transit)
	} else {
		ctx, msg.Transit = lcontext.NewTransitWithContext(ctx)
	}
	// TODO: Create follow up transit

	// Attach contextualised services
	ctx = lcontext.WithTracer(ctx, tracing.FromContext(ctx))
	ctx = lcontext.WithLogger(ctx, log.FromContext(ctx))

	defer func() {
		if recover := recover(); recover != nil {
			log.Err(ctx, "eventdb.pubsub.panic", "Recovered from panic",
				log.Object("err", recover),
				log.String("stack", string(debug.Stack())),
			)
		}
	}()
	defer func() {
		w.msgProcessed()
	}()
	// TODO: Implement retry logic on panic?
	handler(ctx, msg.Data)
}

func (w *worker) Stop() {
	w.stop <- true
}

type message struct {
	Transit lcontext.Transit
	Data    []byte
}

type groupMap map[string]*group

type group struct {
	Subs     []chan *message
	Handlers []pubsub.MsgHandler
}

func (g *group) AddHandler(h pubsub.MsgHandler, buff int) {
	g.Subs = append(g.Subs, make(chan *message, buff))
	g.Handlers = append(g.Handlers, h)
}

func newRegistry() *bg.Reg {
	return bg.NewReg("eventdb.pubsub", context.Background())
}
