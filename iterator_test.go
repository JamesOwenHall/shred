package shred

import (
	"errors"
)

var ErrFailingIterator = errors.New("failing iterator")

type RecordIterator []Record

func (r *RecordIterator) Next() (Record, error) {
	if len(*r) == 0 {
		return nil, nil
	}

	rec := (*r)[0]
	*r = (*r)[1:]
	return rec, nil
}

type FailingIterator struct{}

func (f *FailingIterator) Next() (Record, error) {
	return nil, ErrFailingIterator
}
