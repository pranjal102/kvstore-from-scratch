package kvstorefromscratch

import (
	"bufio"
	"io"
	"os"
)

type FileIterator struct {
	scanner    *bufio.Scanner
	openedfile *os.File
	currPos    int
}

func newFileIterator(openedfile *os.File, offset int64) (*FileIterator, error) {
	_, err := openedfile.Seek(offset, io.SeekStart) // Reset file pointer to the given offset (relative to the start of the file)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(openedfile)
	fileIterator := &FileIterator{
		scanner:    scanner,
		currPos:    -1,
		openedfile: openedfile,
	}
	return fileIterator, nil
}

func (fi *FileIterator) HasNext() bool {
	return fi.scanner.Scan()
}

func (fi *FileIterator) Get() (record, int) {
	var data record
	line := fi.scanner.Text()
	fi.currPos++
	data.FromString(line)
	return data, fi.currPos
}
