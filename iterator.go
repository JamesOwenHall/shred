package shred

type Iterator interface {
	Clone() Iterator
	Next() (Record, error)
}
