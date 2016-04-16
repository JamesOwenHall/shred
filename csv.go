package shred

import (
	"bytes"
	"encoding/csv"
	"io"
	"strconv"
)

type CsvIterator struct {
	input  []byte
	reader *csv.Reader
}

func NewCsvIterator(input []byte) *CsvIterator {
	return &CsvIterator{
		input:  input,
		reader: csv.NewReader(bytes.NewBuffer(input)),
	}
}

func (c *CsvIterator) Clone() Iterator {
	return &CsvIterator{
		input:  c.input,
		reader: csv.NewReader(bytes.NewBuffer(c.input)),
	}
}

func (c *CsvIterator) Next() (Record, error) {
	row, err := c.reader.Read()
	if err == io.EOF {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	next := make(Record)
	for i, val := range row {
		next[strconv.Itoa(i)] = val
	}

	return next, nil
}
