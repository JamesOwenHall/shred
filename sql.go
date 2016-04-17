package shred

import (
	"database/sql"
)

type SqlIterator struct {
	query string
	db    *sql.DB
	rows  *sql.Rows
	buf   []interface{}
	cols  []string
}

func NewSqlIterator(db *sql.DB, query string) *SqlIterator {
	return &SqlIterator{
		query: query,
		db:    db,
		rows:  nil,
		buf:   nil,
		cols:  nil,
	}
}

func (s *SqlIterator) Clone() Iterator {
	return NewSqlIterator(s.db, s.query)
}

func (s *SqlIterator) Next() (Record, error) {
	if s.rows == nil {
		var err error
		s.rows, err = s.db.Query(s.query)
		if err != nil {
			return nil, err
		}

		s.cols, err = s.rows.Columns()
		if err != nil {
			s.rows.Close()
			return nil, err
		}

		s.buf = make([]interface{}, len(s.cols))
		for i := range s.buf {
			s.buf[i] = new(string)
		}
	}

	if !s.rows.Next() {
		s.rows.Close()
		return nil, nil
	}

	if err := s.rows.Scan(s.buf...); err != nil {
		s.rows.Close()
		return nil, err
	}

	next := make(Record)
	for i := range s.buf {
		next[s.cols[i]] = *s.buf[i].(*string)
	}

	return next, nil
}
