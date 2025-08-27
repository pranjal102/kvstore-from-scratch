# part02_hash_index

This package implements a simple key-value store with an append-only log and a hash index for fast lookups. It is the second part of a series building a persistent KV store from scratch in Go.

## Features
- **Append-only log:** All operations are recorded in a file for durability.
- **Hash index:** In-memory index for fast key lookups.
- **PUT/DEL operations:** Supports storing and deleting key-value pairs.
- **Persistence:** Data is stored on disk and survives restarts.

## Usage

### 1. Connect to a Store
```go
store, err := ConnectFileStore("/path/to/dbfile")
if err != nil {
    // handle error
}
defer store.Close()
```

### 2. Put a Key-Value Pair
```go
err := store.Put("key", "value")
```

### 3. Get a Value
```go
val, err := store.Get("key")
```

### 4. Delete a Key
```go
err := store.Del("key")
```

## File Structure
- `datafile.go`: Handles file operations and record appending.
- `datafilewriter.go`: Buffered writer for efficient file writes.
- `fileiterator.go`: Sequential file iterator for reading records.
- `filestore.go`: Main store logic, exposes the Store API.
- `hashindex.go`: In-memory hash index for fast key lookups.
- `kvstore.go`: Store interface definition.
- `record.go`: Record and key-value pair structures.
- `benchmark_test.go`, `filestore_test.go`: Tests and benchmarks.

## Running Tests
From the `part02_hash_index` directory:
```sh
go test -v
```

## Benchmark Results

Benchmarks were run on Apple M4 Pro (darwin/arm64):

| Number of Keys | Average Time per Get (ns) | Operations per Second |
|:--------------:|:------------------------:|:---------------------:|
|    10,000      |        1,434             |      ~697,500         |
|   100,000      |        3,639             |      ~274,700         |
|  1,000,000     |       16,600             |      ~60,200          |

**Interpretation:**
- The hash index enables much faster Get operations compared to the append-only log approach.
- Even with 1 million keys, lookups remain efficient and scale well.

## Notes
- The hash index is rebuilt from the log file on startup.
- The log file is never truncated; deleted keys are marked with a DEL operation.
