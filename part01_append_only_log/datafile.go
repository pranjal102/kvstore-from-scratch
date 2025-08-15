package kvstorefromscratch

import (
	"bufio"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const (
	PRIMARY_FILENAME = "my.db"
	TEMP_FILENAME    = "tmp.db"
)

type DataFile struct {
	dir      string
	fullpath string
	file     *os.File
}

// NewDataFile creates a new DataFile instance by opening or creating the primary data file
// at the specified directory path. It returns a pointer to the DataFile and an error if
// the file cannot be opened or created.
//
// Parameters:
//
//	path - The directory path where the data file should be located.
//
// Returns:
//
//	*DataFile - Pointer to the created DataFile instance.
//	error     - Error encountered during file opening or creation, or nil if successful.
func NewDataFile(path string) (*DataFile, error) {
	fullPath := filepath.Join(path, PRIMARY_FILENAME)

	f, err := os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		dir:      path,
		fullpath: fullPath,
		file:     f,
	}, nil
}

// IsEmpty checks whether the data file at the specified path is empty.
// It returns true if the file exists and its size is zero, otherwise false.
// If there is an error retrieving the file information, it returns false.
func (df *DataFile) IsEmpty() bool {
	info, err := os.Stat(df.fullpath)
	if err != nil {
		return false
	}
	return info.Size() == 0
}

// GetIterator returns a new FileIterator starting at the specified offset within the DataFile.
// It provides sequential access to the file's contents from the given position.
// If the iterator cannot be created, an error is returned.
func (df *DataFile) GetIterator(offset int64) (*FileIterator, error) {
	return newFileIterator(df.file, offset)
}

// NewSibblingFile creates a new sibling data file in the same directory as the current DataFile.
// The new file is created with a temporary filename defined by TEMP_FILENAME.
// It returns a pointer to the newly created DataFile and an error if the file creation fails.
func (df *DataFile) NewSibblingFile() (*DataFile, error) {
	fullPath := filepath.Join(df.dir, TEMP_FILENAME)
	f, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}

	return &DataFile{
		dir:      df.dir,
		fullpath: fullPath,
		file:     f,
	}, nil
}

func (df *DataFile) ReplaceWith(newFile *DataFile) error {

	if err := os.Rename(newFile.fullpath, df.fullpath); err != nil {
		return err
	}
	df.file = newFile.file // Update the current DataFile's file reference to the new file

	return nil
}

// Writer returns a new DatFileWriter instance associated with the DataFile.
// The DatFileWriter uses a buffered writer for efficient writing to the underlying file.
// It returns the DatFileWriter and any error encountered during creation.
func (df *DataFile) Writer() (*DatFileWriter, error) {

	_, err := df.file.Seek(0, io.SeekEnd) // Ensure the file pointer is at the end of the file before writing new records
	if err != nil {
		return nil, err
	}
	return &DatFileWriter{
		writer: bufio.NewWriter(df.file),
	}, nil
}

func (df *DataFile) Open() error {

	file, err := os.OpenFile(df.fullpath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	df.file = file
	return nil
}

func (df *DataFile) Close() error {
	return df.file.Close()
}

func (df *DataFile) ReadRecordAt(offset int64) (*record, error) {
	_, err := df.file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	defer df.file.Seek(0, io.SeekStart) // Reset the file pointer to the start after reading

	scanner := bufio.NewScanner(df.file)
	if scanner.Scan() {
		line := scanner.Text()
		rec := new(record)
		rec.FromString(line)
		return rec, nil
	}
	if scanner.Err() == nil { // If no error but no lines were read, return nil
		return nil, errors.New("no record found at the specified offset")
	}
	return nil, scanner.Err()
}
