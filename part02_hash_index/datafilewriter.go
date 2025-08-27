package kvstorefromscratchpart2

import (
	"bufio"
	"fmt"
)

type DatFileWriter struct {
	writer *bufio.Writer
}

func (dfw *DatFileWriter) Append(data record) (int64, error) {
	record := data.String()
	var bytes int
	var err error
	if bytes, err = fmt.Fprintln(dfw.writer, record); err != nil {
		return int64(bytes), err
	}
	return int64(bytes), nil
}

func (dfw *DatFileWriter) Flush() error {
	return dfw.writer.Flush()
}
