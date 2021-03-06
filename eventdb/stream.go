package eventdb

import (
	"github.com/deixis/errors"
	"github.com/deixis/pkg/utc"
	"github.com/deixis/storage/eventdb/eventpb"
	"github.com/deixis/storage/kvdb"
)

var (
	// ErrStreamNotFound when a requested stream does not exist or has been removed.
	ErrStreamNotFound = errors.WithNotFound(errors.New("stream not found"))
	// ErrSubscriptionNotFound when a requested subscription does not exist or has been removed.
	ErrSubscriptionNotFound = errors.WithNotFound(errors.New("subscription not found"))

	// ErrNoSnapshot occurs when attempting to load a snapshot that does not exist
	ErrNoSnapshot = errors.WithNotFound(errors.New("no snapshot found"))
)

type Stream interface {
	StreamReader
	StreamWriter
}

type StreamReader interface {
	// Event loads a single event from the stream.
	Event(event uint64) (*RecordedEvent, error)
	// Events loads a range of events from the stream.
	Events(start uint64, options ...RangeOption) EventsRangeResult
	// Events loads a range of events from the stream.
	EventsInRange(start, end uint64, options ...RangeOption) EventsRangeResult

	// Snapshots loads a range of snapshots from the stream.
	Snapshots(start uint64, options ...RangeOption) SnapshotsRangeResult
	// ClosestSnapshot returns the closest snapshot recorded before or at
	// the given stream version.
	ClosestSnapshot(version uint64) (*RecordedSnapshot, error)

	// Subsriptions returns all persistent subscriptions from this stream.
	Subscriptions() SubscriptionsRangeResult
	// Subsription returns a persistent subscription
	Subscription(group string) (Subscription, error)

	// Metadata returns a stream metadata
	Metadata() (StreamMetadata, error)
}

type StreamWriter interface {
	// AppendEvents appends events to the stream in ascending order.
	// expectedVersion is the version at which you expect the stream to be in
	// order that an optimistic concurrency check can be performed.
	AppendEvents(expectedVersion uint64, events ...*RecordedEvent) error

	// SetSnapshot stores a snapshot for the given stream version.
	SetSnapshot(version uint64, snap *RecordedSnapshot) error
	// ClearSnapshot permanently removes the snapshot from the stream.
	ClearSnapshot(version uint64)

	// CreateSubscription creates a persistent subscription.
	//
	// Before interacting with a subscription group, you need to create one.
	// You will receive an error if you attempt to create a subscription group
	// more than once.
	//
	// TODO: Add subscription option (e.g retry, ...)
	CreateSubscription(group string) (Subscription, error)
	// DeleteSubscription deletes a persistent subscription
	DeleteSubscription(group string) error

	// SetExtendedMeta adds the given extended metadata do the stream metadata
	//
	// Matching keys will either be created or updated. The remaining keys will be
	// untouched.
	SetExtendedMeta(extended map[string]string) error
	// DeleteExtendedMeta removes the given extended metadata keys from the
	// stream metadata
	DeleteExtendedMeta(keys ...string) error

	// Delete soft deletes a stream. This means you can restore it later.
	// If you try to Load events from a soft deleted stream you receive a not
	// found error.
	Delete() error
	// Restore restores a soft deleted stream. Attempts to restore a non-deleted
	// stream will have no effects.
	Restore() error
	// PermanentlyDelete permanently deletes a stream.
	PermanentlyDelete()
}

type StreamMetadata struct {
	// Key is the canonical stream identifier
	Key kvdb.Key
	// ID is the unique stream identifier within its scope
	ID string
	// Version is the current stream version, which corresponds to the latest
	// event number recorded.
	Version uint64
	// CreationTime is the date on which the stream was created
	CreationTime utc.UTC
	// ModificationTime is the date on which the stream was modified
	ModificationTime utc.UTC
	// DeletionTime is the date on which the stream was soft deleted
	DeletionTime utc.UTC
	// Extended properties
	Extended map[string]string
}

func (m *StreamMetadata) Pack() *eventpb.StreamMetadata {
	return &eventpb.StreamMetadata{
		Key:              m.Key,
		ID:               m.ID,
		Version:          m.Version,
		CreationTime:     int64(m.CreationTime),
		ModificationTime: int64(m.ModificationTime),
		DeletionTime:     int64(m.DeletionTime),
		Extended:         m.Extended,
	}
}

func (m *StreamMetadata) Unpack(s *eventpb.StreamMetadata) {
	m.Key = s.Key
	m.ID = s.ID
	m.Version = s.Version
	m.CreationTime = utc.UTC(s.CreationTime)
	m.ModificationTime = utc.UTC(s.ModificationTime)
	m.DeletionTime = utc.UTC(s.DeletionTime)
	m.Extended = s.Extended
}

type RangeOptions struct {
	Limit   int
	Reverse bool
}

type RangeOption func(opts *RangeOptions)

func WithLimit(l int) RangeOption {
	return func(opts *RangeOptions) {
		opts.Limit = l
	}
}

func WithBackwardRead() RangeOption {
	return func(opts *RangeOptions) {
		opts.Reverse = true
	}
}

type EventsRangeResult interface {
	GetSliceWithError() ([]*RecordedEvent, error)
	Iterator() EventsRangeIterator
}

type EventsRangeIterator interface {
	Advance() bool
	Get() (*RecordedEvent, error)
}

type SnapshotsRangeResult interface {
	GetSliceWithError() ([]*RecordedSnapshot, error)
	Iterator() SnapshotsRangeIterator
}

type SnapshotsRangeIterator interface {
	Advance() bool
	Get() (*RecordedSnapshot, error)
}

type StreamReadersRangeResult interface {
	GetSliceWithError() ([]StreamReader, error)
	Iterator() StreamReadersRangeIterator
}

type StreamReadersRangeIterator interface {
	Advance() bool
	Get() (StreamReader, error)
}

type SubscriptionsRangeResult interface {
	GetSliceWithError() ([]Subscription, error)
	Iterator() SubscriptionsRangeIterator
}

type SubscriptionsRangeIterator interface {
	Advance() bool
	Get() (Subscription, error)
}
