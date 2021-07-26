package eventdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/deixis/errors"
	"github.com/deixis/pkg/utc"
	"github.com/deixis/spine/net/pubsub"
	"github.com/deixis/storage/eventdb/eventpb"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/driver/foundationdb"
	"github.com/golang/protobuf/proto"
)

const (
	// Global namespaces
	nsStream = 0x01
	nsIndex  = 0x02

	// Stream namespaces
	nsStreamEvent        = 0x00
	nsStreamSnapshot     = 0x01
	nsStreamMeta         = 0x02
	nsStreamSubscription = 0x03

	// Index namespaces
	nsIndexStreamID = 0x01
)

var (
	firstKey kvdb.TupleElement
	lastKey  = kvdb.UUID{0xFF}
)

// Store is the EventDB storage interface
type Store interface {
	// Subscribe subscribes to a stream
	Subscribe(
		ctx context.Context,
		streamID string,
		group string,
		subType SubscriptionType,
		h EventHandler,
	) (Subscription, error)

	// Transact initiates a transaction
	Transact(ctx context.Context, f func(Transaction) (interface{}, error)) (interface{}, error)
	// Transact initiates a read-only transaction
	ReadTransact(ctx context.Context, f func(ReadTransaction) (interface{}, error)) (interface{}, error)

	// WithTransaction wraps a KV transaction into an EventDB transaction
	WithTransaction(tx kvdb.Transaction) Transaction
	// WithReadTransaction wraps a KV read transaction into an EventDB read transaction
	WithReadTransaction(tx kvdb.ReadTransaction) ReadTransaction

	// Subspace returns the store current subspace
	Subspace() kvdb.Subspace
	// KV returns the underlying KV store instance
	KV() kvdb.Store

	// Close closes the store
	Close() error
}

// ReadTransaction is a read-only transaction
type ReadTransaction interface {
	// Stream loads a read-only stream from the store
	ReadStream(id string) StreamReader
	// ReadStreams returns a range of streamss
	ReadStreams(opts ...kvdb.RangeOption) StreamReadersRangeResult

	// ReadTransaction returns the underlying KV read transaction
	ReadTransaction() kvdb.ReadTransaction
}

// WriteTransaction is a read-write transaction
type WriteTransaction interface {
	Stream(id string) Stream
	CreateStream(id string, extended ...map[string]string) (Stream, error)
}

// Transaction is a read-write transaction
type Transaction interface {
	ReadTransaction
	WriteTransaction

	// Transaction returns the underlying KV transaction
	Transaction() kvdb.Transaction
}

type EventHandler func(ctx context.Context, e Event) error

type store struct {
	mu sync.Mutex

	kv kvdb.Store
	ss kvdb.Subspace

	pubSub      pubsub.PubSub
	drainPubSub bool
	watchers    map[string][]*subscriptionWatcher
}

// New creates a new EventDB store
func New(
	s kvdb.Store,
	ss kvdb.Subspace,
	storeOpts ...StoreOption,
) (Store, error) {
	opts := defaultStoreOptions()
	for _, opt := range storeOpts {
		opt.apply(opts)
	}

	eventStore := &store{
		kv:          s,
		ss:          ss,
		pubSub:      opts.pubSub,
		drainPubSub: opts.drainPubSub,
		watchers:    map[string][]*subscriptionWatcher{},
	}

	return eventStore, nil
}

func (s *store) Subscribe(
	ctx context.Context,
	streamID, group string,
	subType SubscriptionType,
	h EventHandler,
) (sub Subscription, err error) {
	// Check whether we have a persisted subscription
	var streamMeta *StreamMetadata
	_, err = s.ReadTransact(ctx, func(tx ReadTransaction) (interface{}, error) {
		stream := tx.ReadStream(streamID)
		meta, err := stream.Metadata()
		if err != nil {
			return nil, err
		}
		streamMeta = &meta

		sub, err = stream.Subscription(group)
		if err != nil {
			// An error is expected for transient subscriptions
			return nil, err
		}
		return nil, nil
	})
	switch err {
	case nil:
		// Persisted sub
		if subType != SubscriptionTypePersistent {
			return nil, errors.New("error creating transient subscription as there is already a persisted subscription with the same group name")
		}
	case ErrSubscriptionNotFound:
		// Transient sub
		switch subType {
		case SubscriptionTypeCatchUp:
			sub = newCatchUpSubscription(streamMeta, group)
		case SubscriptionTypeVolatile:
			sub = newVolatileSubscription(streamMeta, group)
		case SubscriptionTypePersistent:
			return nil, err
		default:
			return nil, fmt.Errorf("error creating subscription with type <%s>", subType)
		}
	default:
		return nil, err
	}

	subMeta, err := sub.Meta()
	if err != nil {
		return nil, err
	}

	w := newSubscriptionWatcher()
	w.StreamMeta = streamMeta
	w.SubMeta = &subMeta
	w.Sub = s.pubSub
	w.Store = s
	w.EventHandler = h

	go w.Start(ctx)

	s.mu.Lock()
	s.watchers[streamID] = append(s.watchers[streamID], w)
	s.mu.Unlock()

	return w, nil
}

func (s *store) Transact(
	ctx context.Context, fn func(Transaction) (interface{}, error),
) (interface{}, error) {
	// Execute transaction first to make sure subscribers receive only commited changes
	return s.kv.Transact(ctx, func(kvtx kvdb.Transaction) (interface{}, error) {
		return fn(transact(kvtx, s.ss, s.pubSub))
	})
}

func (s *store) ReadTransact(
	ctx context.Context, fn func(ReadTransaction) (interface{}, error),
) (interface{}, error) {
	return s.kv.ReadTransact(ctx, func(kvtx kvdb.ReadTransaction) (interface{}, error) {
		return fn(readTransact(kvtx, s.ss))
	})
}

func (s *store) WithTransaction(tx kvdb.Transaction) Transaction {
	return transact(tx, s.ss, s.pubSub)
}

func (s *store) WithReadTransaction(tx kvdb.ReadTransaction) ReadTransaction {
	return readTransact(tx, s.ss)
}

func (s *store) Subspace() kvdb.Subspace {
	return s.ss
}

func (s *store) KV() kvdb.Store {
	return s.kv
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all subscription watchers
	for _, watchers := range s.watchers {
		for _, watcher := range watchers {
			watcher.Close()
		}
	}

	// Only drain internal pub sub instances
	if s.drainPubSub {
		s.pubSub.Drain()
	}

	// Finally close KV store
	return s.kv.Close()
}

func readTransact(tx kvdb.ReadTransaction, ss kvdb.Subspace) ReadTransaction {
	return &readTransaction{T: tx, Ss: ss}
}

type readTransaction struct {
	T  kvdb.ReadTransaction
	Ss kvdb.Subspace
}

func (tx *readTransaction) ReadStream(id string) StreamReader {
	return &streamReadTransaction{
		Tx: tx.T,
		Ss: tx.Ss,
		ID: id,
	}
}

func (tx *readTransaction) ReadStreams(opts ...kvdb.RangeOption) StreamReadersRangeResult {
	keyRange := kvdb.KeyRange{
		Begin: key(tx.Ss, nsIndex, nsIndexStreamID, firstKey),
		End:   key(tx.Ss, nsIndex, nsIndexStreamID, lastKey),
	}
	res := tx.T.GetRange(
		keyRange,
		append(
			[]kvdb.RangeOption{foundationdb.WithRangeStreamingMode(fdb.StreamingModeIterator)},
			opts...,
		)...,
	)
	return &streamReadersRangeResult{R: res, Tx: tx.T, Ss: tx.Ss}
}

func (tx *readTransaction) ReadTransaction() kvdb.ReadTransaction {
	return tx.T
}

func transact(
	tx kvdb.Transaction, ss kvdb.Subspace, pubSub pubsub.PubSub,
) Transaction {
	pub := &transactionPublisher{}

	t := &transaction{T: tx, Ss: ss, Pub: pub}
	t.readTransaction.T = tx
	t.readTransaction.Ss = ss

	// Publish all events from the transaction
	tx.AfterCommit(func(ctx context.Context) {
		for i := range pub.events {
			err := pubSub.Publish(ctx, pub.events[i].ch, pub.events[i].data)
			if err != nil {
				// TODO: Implement retry logic?
				return
			}
		}
	})

	return t
}

type transaction struct {
	readTransaction

	T   kvdb.Transaction
	Ss  kvdb.Subspace
	Pub *transactionPublisher
}

func (tx *transaction) Stream(id string) Stream {
	return buildStreamTransaction(tx.T, tx.Ss, tx.Pub, id)
}

func (tx *transaction) CreateStream(
	id string, extended ...map[string]string,
) (Stream, error) {
	if id == "" {
		return nil, errors.Bad(&errors.FieldViolation{
			Field:       "id",
			Description: "Stream ID cannot be empty",
		})
	}

	streamKey := key(tx.Ss, nsStream, id)
	streamMetaKey := key(tx.Ss, nsStream, id, nsStreamMeta)

	rec, err := tx.T.Get(streamMetaKey).Get()
	if err != nil {
		return nil, errors.Wrap(err, "error loading recorded event")
	}
	if len(rec) > 0 {
		return nil, errors.Aborted(&errors.ConflictViolation{
			Resource:    "stream:" + id,
			Description: "Stream has already been created",
		})
	}

	meta := &eventpb.StreamMetadata{
		Key:          streamKey,
		ID:           id,
		Version:      0,
		CreationTime: int64(utc.Now()),
		Extended:     make(map[string]string),
	}
	if len(extended) > 0 && extended[0] != nil {
		for k, v := range extended[0] {
			meta.Extended[k] = v
		}
	}
	md, err := proto.Marshal(meta)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling stream metadata")
	}
	tx.T.Set(streamMetaKey, md)

	streamTx := buildStreamTransaction(tx.T, tx.Ss, tx.Pub, id)
	streamTx.createIndices()
	return streamTx, nil
}

func (tx *transaction) Transaction() kvdb.Transaction {
	return tx.T
}

type transactionPublisher struct {
	events []*pubEvent
}

func (p *transactionPublisher) Publish(ch string, data []byte) {
	p.events = append(p.events, &pubEvent{ch: ch, data: data})
}

type pubEvent struct {
	ch   string
	data []byte
}

type streamReadTransaction struct {
	ID string

	Tx kvdb.ReadTransaction
	Ss kvdb.Subspace
}

func (tx *streamReadTransaction) Event(event uint64) (*RecordedEvent, error) {
	data, err := tx.Tx.Get(key(tx.Ss, nsStream, tx.ID, nsStreamEvent, event)).Get()
	if err != nil {
		return nil, errors.Wrap(err, "error loading recorded event")
	}
	v := &eventpb.RecordedEvent{}
	if err := proto.Unmarshal(data, v); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling recorded event")
	}
	evt := RecordedEvent(*v)
	return &evt, nil
}

func (tx *streamReadTransaction) Events(start uint64, options ...RangeOption) EventsRangeResult {
	opts := RangeOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	var keyRange kvdb.KeyRange
	if !opts.Reverse {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamEvent, start),
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamEvent, lastKey),
		}
	} else {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamEvent, firstKey),
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamEvent, start),
		}
	}

	res := tx.Tx.GetRange(
		keyRange,
		kvdb.WithRangeLimit(opts.Limit),
		foundationdb.WithRangeStreamingMode(fdb.StreamingModeWantAll),
	)
	return &eventsRangeResult{R: res}
}

func (tx *streamReadTransaction) EventsInRange(start, end uint64, options ...RangeOption) EventsRangeResult {
	opts := RangeOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	var keyRange kvdb.KeyRange
	if !opts.Reverse {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamEvent, start),
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamEvent, end+1),
		}
	} else {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamEvent, end),
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamEvent, start+1),
		}
	}

	res := tx.Tx.GetRange(
		keyRange,
		kvdb.WithRangeLimit(opts.Limit),
		foundationdb.WithRangeStreamingMode(fdb.StreamingModeWantAll),
	)
	return &eventsRangeResult{R: res}
}

func (tx *streamReadTransaction) Snapshots(start uint64, options ...RangeOption) SnapshotsRangeResult {
	opts := RangeOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	var keyRange kvdb.KeyRange
	if !opts.Reverse {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, start),
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, lastKey),
		}
	} else {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, firstKey),
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, start+1),
		}
	}

	res := tx.Tx.GetRange(
		keyRange,
		kvdb.WithRangeLimit(opts.Limit),
		foundationdb.WithRangeStreamingMode(fdb.StreamingModeWantAll),
	)
	return &snapshotsRangeResult{R: res}
}

func (tx *streamReadTransaction) ClosestSnapshot(version uint64) (*RecordedSnapshot, error) {
	keyRange := kvdb.KeyRange{
		Begin: key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, firstKey),
		End:   key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, lastKey),
	}
	res := tx.Tx.GetRange(
		keyRange,
		kvdb.WithRangeLimit(1),
		kvdb.WithRangeReverse(true),
		foundationdb.WithRangeStreamingMode(fdb.StreamingModeExact),
	)
	rr := snapshotsRangeResult{R: res}
	snaps, err := rr.GetSliceWithError()
	if err != nil {
		return nil, err
	}
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	return snaps[0], nil
}

func (tx *streamReadTransaction) Metadata() (StreamMetadata, error) {
	meta := StreamMetadata{}
	m, err := tx.loadMetadata()
	if err != nil {
		return meta, err
	}
	meta.Unpack(m)
	return meta, nil
}

func (tx *streamReadTransaction) Subscriptions() SubscriptionsRangeResult {
	keyRange := kvdb.KeyRange{
		Begin: key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, firstKey),
		End:   key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, lastKey),
	}
	res := tx.Tx.GetRange(
		keyRange,
		foundationdb.WithRangeStreamingMode(fdb.StreamingModeIterator),
	)
	return &subscriptionsRangeResult{
		R:  res,
		Ss: tx.Ss,
		Tx: tx.Tx,
	}
}

func (tx *streamReadTransaction) Subscription(group string) (Subscription, error) {
	data, err := tx.Tx.Get(key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, group)).Get()
	if err != nil {
		return nil, errors.Wrap(err, "error loading stream subscription")
	}
	if len(data) == 0 {
		return nil, ErrSubscriptionNotFound
	}
	recordedMeta := eventpb.SubscriptionMetadata{}
	if err := proto.Unmarshal(data, &recordedMeta); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling subscription meta")
	}

	meta := SubscriptionMetadata{}
	meta.Unpack(&recordedMeta)
	return &subscription{
		meta: meta,
	}, nil
}

func (tx *streamReadTransaction) loadMetadata() (*eventpb.StreamMetadata, error) {
	if tx.ID == "" {
		return nil, errors.New("stream ID is empty")
	}

	data, err := tx.Tx.Get(key(tx.Ss, nsStream, tx.ID, nsStreamMeta)).Get()
	if err != nil {
		return nil, errors.Wrap(err, "error loading stream metadata")
	}
	if len(data) == 0 {
		return nil, ErrStreamNotFound
	}
	v := &eventpb.StreamMetadata{}
	if err := proto.Unmarshal(data, v); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling stream metadata")
	}

	// Ensure it is initialised
	if v.Extended == nil {
		v.Extended = make(map[string]string)
	}
	return v, nil
}

type streamTransaction struct {
	streamReadTransaction

	ID string

	Tx kvdb.Transaction
	Ss kvdb.Subspace

	Pub *transactionPublisher
}

func buildStreamTransaction(
	tx kvdb.Transaction, ss kvdb.Subspace, pub *transactionPublisher, id string,
) *streamTransaction {
	t := &streamTransaction{
		ID:  id,
		Tx:  tx,
		Ss:  ss,
		Pub: pub,
	}
	t.streamReadTransaction.Tx = tx
	t.streamReadTransaction.Ss = ss
	t.streamReadTransaction.ID = id
	return t
}

func (tx *streamTransaction) AppendEvents(expectedVersion uint64, events ...*RecordedEvent) error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}
	if expectedVersion != meta.Version {
		return errors.FailedPrecondition(&errors.PreconditionViolation{
			Type:        "version",
			Subject:     "stream/event",
			Description: "Unexpected event version",
		})
	}

	// Append events to stream
	for i, event := range events {
		e := eventpb.RecordedEvent(*event)
		e.Number = meta.Version + uint64(i) + 1

		data, err := proto.Marshal(&e)
		if err != nil {
			return errors.Wrap(err, "error marshalling event")
		}
		tx.Tx.Set(key(tx.Ss, nsStream, tx.ID, nsStreamEvent, e.Number), data)

		// Publish event
		tx.Pub.Publish(tx.ID, data)
	}

	meta.Version += uint64(len(events))
	meta.ModificationTime = int64(utc.Now())
	tx.setMetadata(meta)
	return nil
}

func (tx *streamTransaction) SetSnapshot(version uint64, snap *RecordedSnapshot) error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}
	if version > meta.Version {
		return errors.FailedPrecondition(&errors.PreconditionViolation{
			Type:        "version",
			Subject:     "stream/snapshot",
			Description: "Attempt to record a snapshot for a missing event",
		})
	}

	snap.Number = version

	s := eventpb.RecordedSnapshot(*snap)
	data, err := proto.Marshal(&s)
	if err != nil {
		return errors.Wrap(err, "error marshalling snapshot")
	}
	tx.Tx.Set(key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, version), data)
	return nil
}

func (tx *streamTransaction) ClearSnapshot(version uint64) {
	tx.Tx.Clear(key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, version))
}

func (tx *streamTransaction) CreateSubscription(group string) (Subscription, error) {
	if _, err := tx.loadMetadata(); err != nil {
		return nil, err
	}

	subKey := key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, group)
	data, err := tx.Tx.Get(subKey).Get()
	if err != nil {
		return nil, errors.Wrap(err, "error loading stream subscription")
	}
	if len(data) > 0 {
		return nil, errors.Aborted(&errors.ConflictViolation{
			Resource: fmt.Sprintf("stream:%s/subscription:%s",
				tx.ID,
				group,
			),
			Description: "Subscription has already been created",
		})
	}

	now := int64(utc.Now())
	recordedMeta := eventpb.SubscriptionMetadata{
		Key:              subKey,
		Group:            group,
		StreamID:         tx.ID,
		Type:             eventpb.SubscriptionType_Persistent,
		Position:         0, // Start from the beginning by default
		CreationTime:     now,
		ModificationTime: now,
		DeletionTime:     0,
		Extended:         map[string]string{},
	}
	data, err = proto.Marshal(&recordedMeta)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling subscription meta")
	}
	tx.Tx.Set(key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, group), data)

	meta := SubscriptionMetadata{}
	meta.Unpack(&recordedMeta)
	return &subscription{
		meta: meta,
	}, nil
}

func (tx *streamTransaction) DeleteSubscription(group string) error {
	// Pull subscription from storage
	subKey := key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, group)
	data, err := tx.Tx.Get(subKey).Get()
	if err != nil {
		return errors.Wrap(err, "error loading stream subscription")
	}
	if len(data) == 0 {
		return ErrSubscriptionNotFound
	}
	subMeta := eventpb.SubscriptionMetadata{}
	if err := proto.Unmarshal(data, &subMeta); err != nil {
		return errors.Wrap(err, "error unmarshalling subscription meta")
	}

	// Validation checks
	if subMeta.DeletionTime != 0 {
		return errors.Aborted(&errors.ConflictViolation{
			Resource: fmt.Sprintf("stream:%s/subscription:%s",
				tx.ID,
				group,
			),
			Description: "Subscription has already been deleted",
		})
	}

	// Update subscription metadata
	now := int64(utc.Now())
	subMeta.ModificationTime = now
	subMeta.DeletionTime = now

	// TODO: Disconnect subscriptions

	// Persist back to store
	data, err = proto.Marshal(&subMeta)
	if err != nil {
		return errors.Wrap(err, "error marshalling subscription meta")
	}
	tx.Tx.Set(key(tx.Ss, nsStream, tx.ID, nsStreamSubscription, group), data)
	return nil
}

func (tx *streamTransaction) Delete() error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}
	if meta.DeletionTime != 0 {
		return errors.Aborted(&errors.ConflictViolation{
			Resource:    "stream:" + meta.ID,
			Description: "Stream has already been deleted",
		})
	}

	now := int64(utc.Now())
	meta.ModificationTime = now
	meta.DeletionTime = now
	tx.setMetadata(meta)

	tx.deleteIndices()
	tx.closeStreamWatchers()
	return nil
}

func (tx *streamTransaction) Restore() error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}
	if meta.DeletionTime == 0 {
		return nil // Stream has not been deleted
	}

	now := int64(utc.Now())
	meta.ModificationTime = now
	meta.DeletionTime = 0
	tx.setMetadata(meta)

	tx.createIndices()
	return nil
}

func (tx *streamTransaction) PermanentlyDelete() {
	// Close watchers first to make sure they don't write on the stream namespace
	// after clear range
	tx.closeStreamWatchers()

	// Cleanup stream namespace
	tx.Tx.ClearRange(kvdb.KeyRange{
		Begin: key(tx.Ss, nsStream, tx.ID, firstKey),
		End:   key(tx.Ss, nsStream, tx.ID, lastKey),
	})

	tx.deleteIndices()
}

// SetExtendedMeta adds the given extended metadata do the stream metadata
//
// Matching keys will either be created or updated. The remaining keys will be
// untouched.
func (tx *streamTransaction) SetExtendedMeta(extended map[string]string) error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}

	for k, v := range extended {
		meta.Extended[k] = v
	}

	tx.setMetadata(meta)
	return nil
}

// DeleteExtendedMeta removes the given extended metadata keys from the
// stream metadata
func (tx *streamTransaction) DeleteExtendedMeta(keys ...string) error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}

	for _, key := range keys {
		delete(meta.Extended, key)
	}

	tx.setMetadata(meta)
	return nil
}

func (tx *streamTransaction) closeStreamWatchers() {
	// TODO: Close all stream watchers
}

func (tx *streamTransaction) createIndices() {
	tx.Tx.Set(key(tx.Ss, nsIndex, nsIndexStreamID, tx.ID), nil)
}

func (tx *streamTransaction) deleteIndices() {
	tx.Tx.Clear(key(tx.Ss, nsIndex, nsIndexStreamID, tx.ID))
}

func (tx *streamTransaction) setMetadata(meta *eventpb.StreamMetadata) {
	data, err := proto.Marshal(meta)
	if err != nil {
		panic(errors.Wrap(err, "error marshalling stream metadata"))
	}
	tx.Tx.Set(key(tx.Ss, nsStream, tx.ID, nsStreamMeta), data)
}

func key(ss kvdb.Subspace, e ...kvdb.TupleElement) kvdb.Key {
	return ss.Pack(e)
}

type eventsRangeResult struct {
	R kvdb.RangeResult
}

func (r *eventsRangeResult) GetSliceWithError() (events []*RecordedEvent, err error) {
	i := r.Iterator()
	for i.Advance() {
		evt, err := i.Get()
		if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
	return events, nil
}

func (r *eventsRangeResult) Count() (count int) {
	i := r.Iterator()
	for i.Advance() {
		count++
	}
	return count
}

func (r *eventsRangeResult) Iterator() EventsRangeIterator {
	return &eventsRangeIterator{I: r.R.Iterator()}
}

type eventsRangeIterator struct {
	I kvdb.RangeIterator
}

func (i *eventsRangeIterator) Advance() bool {
	return i.I.Advance()
}

func (i *eventsRangeIterator) Get() (*RecordedEvent, error) {
	kv, err := i.I.Get()
	if err != nil {
		return nil, err
	}

	v := eventpb.RecordedEvent{}
	if err := proto.Unmarshal(kv.Value, &v); err != nil {
		return nil, err
	}
	evt := RecordedEvent(v)
	return &evt, nil
}

type snapshotsRangeResult struct {
	R kvdb.RangeResult
}

func (r *snapshotsRangeResult) GetSliceWithError() (snapshots []*RecordedSnapshot, err error) {
	i := r.Iterator()
	for i.Advance() {
		snap, err := i.Get()
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, nil
}

func (r *snapshotsRangeResult) Iterator() SnapshotsRangeIterator {
	return &snapshotsRangeIterator{I: r.R.Iterator()}
}

type snapshotsRangeIterator struct {
	I kvdb.RangeIterator
}

func (i *snapshotsRangeIterator) Advance() bool {
	return i.I.Advance()
}

func (i *snapshotsRangeIterator) Get() (*RecordedSnapshot, error) {
	kv, err := i.I.Get()
	if err != nil {
		return nil, err
	}

	v := eventpb.RecordedSnapshot{}
	if err := proto.Unmarshal(kv.Value, &v); err != nil {
		return nil, err
	}
	evt := RecordedSnapshot(v)
	return &evt, nil
}

type streamReadersRangeResult struct {
	R  kvdb.RangeResult
	Ss kvdb.Subspace
	Tx kvdb.ReadTransaction
}

func (r *streamReadersRangeResult) GetSliceWithError() (streamReaders []StreamReader, err error) {
	i := r.Iterator()
	for i.Advance() {
		liq, err := i.Get()
		if err != nil {
			return nil, err
		}
		streamReaders = append(streamReaders, liq)
	}
	return streamReaders, nil
}

func (r *streamReadersRangeResult) Count() (count int) {
	i := r.Iterator()
	for i.Advance() {
		count++
	}
	return count
}

func (r *streamReadersRangeResult) Iterator() StreamReadersRangeIterator {
	return &streamReadersRangeIterator{I: r.R.Iterator(), Tx: r.Tx, Ss: r.Ss}
}

type streamReadersRangeIterator struct {
	I  kvdb.RangeIterator
	Ss kvdb.Subspace
	Tx kvdb.ReadTransaction
}

func (i *streamReadersRangeIterator) Advance() bool {
	return i.I.Advance()
}

func (i *streamReadersRangeIterator) Get() (StreamReader, error) {
	kv, err := i.I.Get()
	if err != nil {
		return nil, err
	}
	tuple, err := i.Ss.Unpack(kv.Key)
	if err != nil {
		return nil, errors.Wrapf(err, "error unpacking key %s", kv.Key)
	}
	if len(tuple) < 2 {
		return nil, errors.Wrapf(err, "error invalid tuple key %s", kv.Key)
	}

	id := tuple[len(tuple)-1].(string)
	return &streamReadTransaction{
		Tx: i.Tx,
		Ss: i.Ss,
		ID: id,
	}, nil
}

type subscriptionsRangeResult struct {
	R  kvdb.RangeResult
	Ss kvdb.Subspace
	Tx kvdb.ReadTransaction
}

func (r *subscriptionsRangeResult) GetSliceWithError() (subscriptions []Subscription, err error) {
	i := r.Iterator()
	for i.Advance() {
		liq, err := i.Get()
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, liq)
	}
	return subscriptions, nil
}

func (r *subscriptionsRangeResult) Count() (count int) {
	i := r.Iterator()
	for i.Advance() {
		count++
	}
	return count
}

func (r *subscriptionsRangeResult) Iterator() SubscriptionsRangeIterator {
	return &subscriptionsRangeIterator{I: r.R.Iterator(), Tx: r.Tx, Ss: r.Ss}
}

type subscriptionsRangeIterator struct {
	I  kvdb.RangeIterator
	Ss kvdb.Subspace
	Tx kvdb.ReadTransaction
}

func (i *subscriptionsRangeIterator) Advance() bool {
	return i.I.Advance()
}

func (i *subscriptionsRangeIterator) Get() (Subscription, error) {
	kv, err := i.I.Get()
	if err != nil {
		return nil, err
	}
	recordedMeta := eventpb.SubscriptionMetadata{}
	if err := proto.Unmarshal(kv.Value, &recordedMeta); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling subscription meta")
	}

	meta := SubscriptionMetadata{}
	meta.Unpack(&recordedMeta)
	return &subscription{
		meta: meta,
	}, nil
}
