package eventdb

import (
	"github.com/deixis/pkg/utc"
	"github.com/deixis/storage/eventdb/eventpb"
	"github.com/deixis/storage/kvdb"
	"github.com/deixis/errors"
)

var (
	// ErrStreamNotFound when a requested stream does not exist or has been removed.
	ErrStreamNotFound = errors.WithNotFound(errors.New("stream not found"))

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

	// Snapshots loads a range of snapshots from the stream.
	Snapshots(start uint64, options ...RangeOption) SnapshotsRangeResult
	// ClosestSnapshot returns the closest snapshot recorded before or at
	// the given stream version.
	ClosestSnapshot(version uint64) (*RecordedSnapshot, error)

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

	// Delete soft deletes a stream. This means you can restore it later.
	// If you try to Load events from a soft deleted stream you receive a not
	// found error.
	Delete() error
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
