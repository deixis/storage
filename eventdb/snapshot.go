package eventdb

import (
	"github.com/deixis/storage/eventdb/eventpb"
)

// A RecordedSnapshot is a stored object representing an aggregate at a given version
type RecordedSnapshot eventpb.RecordedSnapshot
