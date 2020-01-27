package eventdb

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/deixis/errors"
	"github.com/deixis/pkg/utc"
	"github.com/deixis/storage/eventdb/eventpb"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/driver/foundationdb"
	"github.com/gogo/protobuf/proto"
)

const (
	// Global namespaces
	nsStream = 0x01
	nsIndex  = 0x02

	// Stream namespaces
	nsStreamEvent    = 0x00
	nsStreamSnapshot = 0x01
	nsStreamMeta     = 0x02

	// Index namespaces
	nsIndexStreamID = 0x01
)

var (
	firstKey tuple.TupleElement
	lastKey  = tuple.UUID{0xFF}
)

func Transact(tx kvdb.Transaction, ss kvdb.Subspace) (Transaction, error) {
	t := &transaction{T: tx, Ss: ss}
	t.readTransaction.T = tx
	t.readTransaction.Ss = ss
	return t, nil
}

func ReadTransact(tx kvdb.ReadTransaction, ss kvdb.Subspace) (ReadTransaction, error) {
	return &readTransaction{T: tx, Ss: ss}, nil
}

// Store is the EventDB storage interface
type Store interface {
	Transact(ctx context.Context, f func(Transaction) (interface{}, error)) (interface{}, error)
	ReadTransact(ctx context.Context, f func(ReadTransaction) (interface{}, error)) (interface{}, error)

	WithTransact(kvdb.Transaction) Transaction
	WithReadTransact(kvdb.ReadTransaction) ReadTransaction

	CreateOrOpenDir(path []string) (kvdb.DirectorySubspace, error)
	Close() error
}

// ReadTransaction is a read-only transaction
type ReadTransaction interface {
	// Stream loads a read-only stream from the store
	ReadStream(id string) StreamReader
	// ReadStreams returns a range of streamss
	ReadStreams(opts ...kvdb.RangeOption) StreamReadersRangeResult
}

// WriteTransaction is a read-write transaction
type WriteTransaction interface {
	Stream(id string) Stream
	CreateStream(id string) (Stream, error)
}

// Transaction is a read-write transaction
type Transaction interface {
	ReadTransaction
	WriteTransaction
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

type transaction struct {
	readTransaction

	T  kvdb.Transaction
	Ss kvdb.Subspace
}

func (tx *transaction) Stream(id string) Stream {
	return buildStreamTransaction(tx.T, tx.Ss, id)
}

func (tx *transaction) CreateStream(id string) (Stream, error) {
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
	md, err := proto.Marshal(meta)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling stream metadata")
	}
	tx.T.Set(streamMetaKey, md)

	// Add stream index
	indexStreamKey := key(tx.Ss, nsIndex, nsIndexStreamID, id)
	tx.T.Set(indexStreamKey, nil)

	return buildStreamTransaction(tx.T, tx.Ss, id), nil
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
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamEvent, end),
		}
	} else {
		keyRange = kvdb.KeyRange{
			Begin: key(tx.Ss, nsStream, tx.ID, nsStreamEvent, end),
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
			End:   key(tx.Ss, nsStream, tx.ID, nsStreamSnapshot, start),
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
	return v, nil
}

type streamTransaction struct {
	streamReadTransaction

	ID string

	Tx kvdb.Transaction
	Ss kvdb.Subspace
}

func buildStreamTransaction(tx kvdb.Transaction, ss kvdb.Subspace, id string) *streamTransaction {
	t := &streamTransaction{
		ID: id,
		Tx: tx,
		Ss: ss,
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
		tx.Tx.Set(key(tx.Ss, nsStream, tx.ID, nsStreamEvent, expectedVersion+uint64(i)+1), data)
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

func (tx *streamTransaction) Delete() error {
	meta, err := tx.loadMetadata()
	if err != nil {
		return err
	}
	if meta.DeletionTime != 0 {
		return errors.Aborted(&errors.ConflictViolation{
			Resource:    "stream:" + meta.ID,
			Description: "Stream has already been created",
		})
	}

	now := int64(utc.Now())
	meta.ModificationTime = now
	meta.DeletionTime = now
	tx.setMetadata(meta)

	tx.deleteIndices()
	return nil
}

func (tx *streamTransaction) PermanentlyDelete() {
	// Cleanup stream namespace
	tx.Tx.ClearRange(kvdb.KeyRange{
		Begin: key(tx.Ss, nsStream, tx.ID, firstKey),
		End:   key(tx.Ss, nsStream, tx.ID, lastKey),
	})

	tx.deleteIndices()
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
