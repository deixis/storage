package eventdb

import (
	"encoding"
	"fmt"
	"reflect"

	"github.com/deixis/errors"
	"github.com/deixis/storage/eventdb/eventpb"
)

// An Event is an object representing the occurrence of something important.
type Event interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

var (
	// ErrUnregisteredEvent occurs when a recorded event does not have a registered Event
	ErrUnregisteredEvent = errors.New("unregistered eventdb event")
)

// A RecordedEvent is a stored object representing the occurrence of an Event
type RecordedEvent eventpb.RecordedEvent

// Unmarshal unmarshals recorded event data into its registered Event
func (r RecordedEvent) Unmarshal() (Event, error) {
	if !isEventRegistered(r.Name) {
		return nil, ErrUnregisteredEvent
	}
	vPtr := reflect.New(EventType(r.Name))
	v := vPtr.Elem().Interface().(Event)

	if err := v.UnmarshalBinary(r.Data); err != nil {
		return nil, err
	}
	return v, nil
}

// Marshal marshals the given event to the recorded event data and set its EventType
func (r RecordedEvent) Marshal(e Event) error {
	data, err := e.MarshalBinary()
	if err != nil {
		return err
	}

	r.Name = EventName(e)
	r.Data = data
	return nil
}

// A registry of all linked message types.
// The string is a fully-qualified event name ("pkg.Event").
var (
	eventTypedNils = make(map[string]Event) // a map from event names to typed nil pointers
	revEventTypes  = make(map[reflect.Type]string)
)

// RegisterEvent registers an `Event` from the fully qualified event name
// to the type (pointer to struct).
func RegisterEvent(x Event, name string) {
	if _, ok := eventTypedNils[name]; ok {
		panic(fmt.Sprintf("event: duplicate event type registered: %s", name))
	}
	t := reflect.TypeOf(x)
	if v := reflect.ValueOf(x); v.Kind() == reflect.Ptr && v.Pointer() == 0 {
		// Generated code always calls RegisterType with nil x.
		// This check is just for extra safety.
		eventTypedNils[name] = x
	} else {
		eventTypedNils[name] = reflect.Zero(t).Interface().(Event)
	}
	revEventTypes[t] = name
}

// EventName returns the fully-qualified event name for the given message type.
func EventName(e Event) string {
	return revEventTypes[reflect.TypeOf(e)]
}

// EventType returns the event type (pointer to struct) for a named event.
// The type is not guaranteed to implement event.Event if the name refers to a
// map entry.
func EventType(name string) reflect.Type {
	if t, ok := eventTypedNils[name]; ok {
		return reflect.TypeOf(t)
	}
	return nil
}

func isEventRegistered(name string) bool {
	_, ok := eventTypedNils[name]
	return ok
}
