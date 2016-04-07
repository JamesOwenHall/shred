package shred

type Iterator interface {
	Next() (Record, error)
}
