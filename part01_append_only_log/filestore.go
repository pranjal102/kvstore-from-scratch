package kvstorefromscratch

import (
	"errors"
	"os"
	"path/filepath"
)

const (
	OPERATION_PUT = "PUT"
	OPERATION_DEL = "DEL"
)

var (
	ErrKeyDoesntExist = errors.New("given key doesn't exist")
)

type FileStore struct {
	dbFile *DataFile
}

//TODO : file store should be able to do the following:
// 1) Read DB file and populate the hash index
// 2) If DB file exits then do not overwrite it, just open it and use it.

// ConnectFileStore initializes and returns a new FileStore instance at the specified file path.
// It ensures that the directory for the file exists, creating it if necessary.
// If the directory cannot be created or the data file cannot be opened, an error is returned.
// On success, it returns a Store backed by the file at the given path.
func ConnectFileStore(path string) (Store, error) {

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	file, err := NewDataFile(path)
	if err != nil {
		return nil, err
	}
	return &FileStore{
		dbFile: file,
	}, nil
}

//TODO - Ensure the file pointer is at the end of the file before writing new records.

// Put stores the given key-value pair in the file store.
// It appends a new record with the specified key and value to the underlying database file.
// Returns an error if writing or flushing the record fails.
func (f *FileStore) Put(K, V string) error {

	writer, err := f.dbFile.Writer()
	if err != nil {
		return err
	}
	dataToAppend := record{
		operation: OPERATION_PUT,
		data:      KVPair{key: K, val: V},
	}
	if _, err = writer.Append(dataToAppend); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

//TODO Use index to get the offset of the key

// Get retrieves the value associated with the given key K from the file store.
// It iterates through the records in the underlying database file, searching for
// a record with the specified key and a PUT operation. If found, it returns the
// corresponding value. If the key does not exist, it returns ErrKeyDoesntExist.
// Returns an error if there is a problem accessing the iterator.
func (f *FileStore) Get(K string) (string, error) {
	iterator, err := f.dbFile.GetIterator(0)
	if err != nil {
		return "", err
	}
	valueForKey := ""
	for iterator.HasNext() {
		record, _ := iterator.Get()
		if record.operation == OPERATION_PUT && record.data.key == K {
			valueForKey = record.data.val
		} else if record.operation == OPERATION_DEL && record.data.key == K { // If a delete operation is found for the key, reset valueForKey
			// This means the key was deleted, so we should not return any value.
			valueForKey = ""
		}
	}
	if valueForKey == "" {
		return "", ErrKeyDoesntExist
	}
	return valueForKey, nil
}

// Del deletes the key-value pair associated with the given key K from the file store.
// It appends a delete operation record to the underlying database file and flushes the changes.
// Returns an error if writing or flushing the record fails.
func (f *FileStore) Del(K string) error {
	writer, err := f.dbFile.Writer()
	if err != nil {
		return err
	}
	dataToAppend := record{
		operation: OPERATION_DEL,
		data:      KVPair{key: K},
	}
	if _, err := writer.Append(dataToAppend); err != nil {
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	return nil
}

// Close closes the underlying database file associated with the FileStore.
// It returns an error if the file cannot be closed.
func (f *FileStore) Close() error {
	return f.dbFile.Close()
}
