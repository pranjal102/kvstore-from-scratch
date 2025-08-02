package kvstorefromscratch

import (
	"bufio"
	"fmt"
)

type DatFileWriter struct {
	writer *bufio.Writer
}

func (dfw *DatFileWriter) Append(data record) (int, error) {
	record := data.String()
	var bytes int
	if bytes, err := fmt.Fprintln(dfw.writer, record); err != nil {
		return bytes, err
	}
	return bytes, nil
}

func (dfw *DatFileWriter) Flush() error {
	return dfw.writer.Flush()
}
