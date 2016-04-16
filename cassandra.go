package shred

import (
	"github.com/gocql/gocql"
)

type CassandraIterator struct {
	session *gocql.Session
	query   string
	iter    *gocql.Iter
}

func NewCassandraIterator(session *gocql.Session, query string) *CassandraIterator {
	return &CassandraIterator{
		session: session,
		query:   query,
		iter:    nil,
	}
}

func (c *CassandraIterator) Clone() Iterator {
	return &CassandraIterator{
		session: c.session,
		query:   c.query,
		iter:    nil,
	}
}

func (c *CassandraIterator) Next() (Record, error) {
	if c.iter == nil {
		c.iter = c.session.Query(c.query).Iter()
	}

	buf := make(map[string]interface{})
	if !c.iter.MapScan(buf) {
		return nil, c.iter.Close()
	}

	// Conform types.
	for k, v := range buf {
		switch v := v.(type) {
		case int8:
			buf[k] = int(v)
		case int16:
			buf[k] = int(v)
		case int32:
			buf[k] = int(v)
		case int64:
			buf[k] = int(v)
		}
	}

	return Record(buf), nil
}
