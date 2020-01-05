package eventdb

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const refSeparator = ":"

// Reference is a versionned identifier
// A version identifier references a specific snapshot of an entity
type Reference string

// BuildReference returns a new Reference for id:version
func BuildReference(id string, version *uint64) Reference {
	if version == nil {
		return Reference(id)
	}
	return Reference(strings.Join(
		[]string{
			id,
			strconv.FormatUint(*version, 10),
		}, refSeparator,
	))
}

// ID returns the identifier
func (r Reference) ID() string {
	return strings.Split(string(r), refSeparator)[0]
}

// String returns a string representation of the whole reference
func (r Reference) String() string {
	return string(r)
}

// Version returns the version of the snapshot (if any).
// If it returns false, you should assume that it references the latest version
// of the entity
func (r Reference) Version() (uint64, bool) {
	parts := strings.Split(string(r), refSeparator)
	if len(parts) > 1 {
		v, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return v, true
	}
	return 0, false
}

// MarshalJSON implements the json.Marshaler interface.
func (r *Reference) MarshalJSON() ([]byte, error) {
	return []byte(strings.Join([]string{"\"", r.String(), "\""}, "")), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (r *Reference) UnmarshalJSON(data []byte) error {
	return r.UnmarshalText(bytes.Trim(data, "\""))
}

// MarshalText implements the encoding.TextMarshaler interface
func (r Reference) MarshalText() (text []byte, err error) {
	return []byte(r.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface
func (r *Reference) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		return nil
	}

	parts := strings.Split(string(text), refSeparator)
	if len(parts) > 1 {
		// Parse version just to make sure it has the correct format
		if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil {
			return errors.Wrap(err, "invalid reference version")
		}

		*r = Reference(strings.Join(
			[]string{
				parts[0],
				parts[1],
			}, refSeparator,
		))
		return nil
	}
	*r = Reference(parts[0])
	return nil
}
