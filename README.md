# Storage

Storage provides various storage layers built on top of a sorted/transactional Key/Value store.

## Layers

  1. [eventdb](./eventdb) - Event Sourcing layer
  2. [kvdb](./kvdb) - Key/Value store

## Key/Value Drivers

  1. Badger
  2. BBolt
  3. FoundationDB

## Dependencies

KV store uses FoundationDB c bindings. Therefore, the FoundationDB client needs to be installed.

https://apple.github.io/foundationdb/downloads.html
