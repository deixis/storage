# KV - Key/Value store

Storage provides various storage layers built on top of a sorted/transactional Key/Value store.

## Middlewares

  1. Crypto
  2. Trace

## Example

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/deixis/storage/kvdb"
	"github.com/deixis/storage/kvdb/driver/bbolt"
	"github.com/deixis/storage/kvdb/kvtrace"
	"github.com/deixis/errors"
)

func main() {
	// Create database client
	os.Mkdir("db", 0770)
	defer os.RemoveAll("db")
	bbs, err := bbolt.Open("./db/test", 0600, "default")
	if err != nil {
		panic(errors.Wrap(err, "error opening DB"))
	}
	ctx := context.TODO()

	// Add trace middleware
	kvs := kvtrace.Trace(bbs)

	// Create directory (subspace)
	// Directories are a recommended approach for administering applications.
	// Each application should create or open at least one directory to manage its subspaces.
	dir, err := kvs.CreateOrOpenDir([]string{"foo"})
	if err != nil {
		panic(errors.Wrap(err, "can't create or open directory"))
	}

	// Read/Write transaction
	_, err = kvs.Transact(ctx, func(tx kvdb.Transaction) (v interface{}, err error) {
		tx.Set(key(dir, "my_key"), []byte("my data"))
		return nil, nil
	})
	if err != nil {
		panic(errors.Wrap(err, "can't start a transaction"))
	}

	// Read-only transaction
	var data []byte
	_, err = kvs.ReadTransact(ctx, func(tx kvdb.ReadTransaction) (v interface{}, err error) {
		data, err = tx.Get(key(dir, "my_key")).Get()
		if err != nil {
			return nil, errors.Wrap(err, "error loading value for key")
		}
		return nil, nil
	})
	if err != nil {
		panic(errors.Wrap(err, "can't start a read transaction"))
  }

  fmt.Println("Got value:", string(data))
}

func key(ss kvdb.Subspace, e ...kvdb.TupleElement) kvdb.Key {
	return ss.Pack(e)
}

```