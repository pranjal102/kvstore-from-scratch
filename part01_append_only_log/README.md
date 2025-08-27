
# part01_append_only_log

This package implements the first part of a persistent key-value store in Go, using an append-only log for durability.

## Features

- **Append-only log:** All operations are recorded sequentially in a file.
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
- `kvstore.go`: Store interface definition.
- `record.go`: Record and key-value pair structures.
- `benchmark_test.go`, `filestore_test.go`: Tests and benchmarks.

## Running Tests

From the `part01_append_only_log` directory:

```sh
go test -v
```

## Benchmark Results

Benchmarks were run on Apple M4 Pro (darwin/arm64):

| Number of Keys | Average Time per Get (ns) | Operations per Second |
|:--------------:|:------------------------:|:---------------------:|
|    10,000      |        433,883           |      ~2,304           |
|   100,000      |      4,434,515           |        ~225           |
|  1,000,000     |     45,755,853           |         ~22           |

**Interpretation:**
- As the number of keys increases, the average time to retrieve a key also increases.
- This is because the append-only log requires scanning through more records to find the latest value for a key.
- We have optimized this by introducing hash index in next part of the series.


