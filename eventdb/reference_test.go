package eventdb_test

import (
	"testing"

	"github.com/deixis/storage/eventdb"
)

func TestReference(t *testing.T) {
	expectIdentifier := "foo123"
	expectVersion := uint64(23)

	ref := eventdb.Reference("foo123")
	if expectIdentifier != ref.ID() {
		t.Errorf("expect identifier %s, but got %s", expectIdentifier, ref.ID())
	}
	if version, ok := ref.Version(); ok {
		t.Errorf("expect no version, but got %d", version)
	}

	ref = eventdb.Reference("foo123:23")
	if expectIdentifier != ref.ID() {
		t.Errorf("expect identifier %s, but got %s", expectIdentifier, ref.ID())
	}
	version, ok := ref.Version()
	if !ok {
		t.Errorf("expect version, but got %d", expectVersion)
	}
	if expectVersion != version {
		t.Errorf("expect version %d, but got %d", expectVersion, version)
	}

	if "foo123:23" != ref.String() {
		t.Errorf("unexpected string %s", ref.String())
	}
}
