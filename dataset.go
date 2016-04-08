package shred

import (
	"sort"
)

type Dataset struct {
	Input     Iterator
	Transform func(Iterator) (Record, error)
}

func NewDataset(input Iterator) *Dataset {
	return &Dataset{
		Input: input,
	}
}

func (d *Dataset) Clone() Iterator {
	return &Dataset{
		Input:     d.Input.Clone(),
		Transform: d.Transform,
	}
}

func (d *Dataset) Next() (Record, error) {
	if d.Transform == nil {
		return d.Input.Next()
	}

	return d.Transform(d.Input)
}

func (d *Dataset) Collect() ([]Record, error) {
	records := []Record{}

	for {
		rec, err := d.Next()
		if err != nil {
			return nil, err
		} else if rec == nil {
			return records, nil
		}

		records = append(records, rec)
	}
}

func (d *Dataset) Filter(fn func(Record) bool) *Dataset {
	return &Dataset{
		Input: d.Clone(),
		Transform: func(iterator Iterator) (Record, error) {
			for {
				next, err := iterator.Next()
				if err != nil {
					return nil, err
				} else if next == nil {
					return nil, nil
				} else if fn(next) {
					return next, nil
				}
			}
		},
	}
}

func (d *Dataset) Map(fn func(Record) Record) *Dataset {
	return &Dataset{
		Input: d.Clone(),
		Transform: func(iterator Iterator) (Record, error) {
			next, err := iterator.Next()
			if err != nil {
				return nil, err
			} else if next == nil {
				return nil, nil
			}

			return fn(next), nil
		},
	}
}

func (d *Dataset) Reduce(fn func(a, b Record) Record) *Dataset {
	var acc Record
	return &Dataset{
		Input: d.Clone(),
		Transform: func(iterator Iterator) (Record, error) {
			for {
				next, err := iterator.Next()
				if err != nil {
					return nil, err
				} else if next == nil {
					result := acc
					acc = nil
					return result, nil
				} else if acc == nil {
					acc = next
					continue
				}

				acc = fn(acc, next)
			}
		},
	}
}

func (d *Dataset) ReduceByKey(key string, fn func(a, b Record) Record) *Dataset {
	var acc []Record
	done := false

	return &Dataset{
		Input: d.Clone(),
		Transform: func(iterator Iterator) (Record, error) {
			if !done {
				keyed := map[interface{}]Record{}
				for {
					next, err := iterator.Next()
					if err != nil {
						return nil, err
					} else if next == nil {
						break
					}

					reduceVal := next.Get(key)
					if a, exists := keyed[reduceVal]; !exists {
						keyed[reduceVal] = next
					} else {
						keyed[reduceVal] = fn(a, next)
					}
				}

				for _, rec := range keyed {
					acc = append(acc, rec)
				}
				done = true
			}

			if len(acc) == 0 {
				return nil, nil
			}

			next := acc[0]
			acc = acc[1:]
			return next, nil
		},
	}
}

func (d *Dataset) SortInt(key string) *Dataset {
	var recs []Record
	done := false

	return &Dataset{
		Input: d.Clone(),
		Transform: func(iterator Iterator) (Record, error) {
			if !done {
				var err error
				if recs, err = NewDataset(iterator).Collect(); err != nil {
					return nil, err
				}

				sort.Sort(intSorter{records: recs, key: key})
				done = true
			}

			if len(recs) == 0 {
				return nil, nil
			}

			next := recs[0]
			recs = recs[1:]
			return next, nil
		},
	}
}

func (d *Dataset) SortString(key string) *Dataset {
	var recs []Record
	done := false

	return &Dataset{
		Input: d.Clone(),
		Transform: func(iterator Iterator) (Record, error) {
			if !done {
				var err error
				if recs, err = NewDataset(iterator).Collect(); err != nil {
					return nil, err
				}

				sort.Sort(stringSorter{records: recs, key: key})
				done = true
			}

			if len(recs) == 0 {
				return nil, nil
			}

			next := recs[0]
			recs = recs[1:]
			return next, nil
		},
	}
}

type intSorter struct {
	records []Record
	key     string
}

func (i intSorter) Len() int {
	return len(i.records)
}

func (i intSorter) Less(a, b int) bool {
	return i.records[a].Int(i.key) < i.records[b].Int(i.key)
}

func (i intSorter) Swap(a, b int) {
	i.records[a], i.records[b] = i.records[b], i.records[a]
}

type stringSorter struct {
	records []Record
	key     string
}

func (s stringSorter) Len() int {
	return len(s.records)
}

func (s stringSorter) Less(a, b int) bool {
	return s.records[a].String(s.key) < s.records[b].String(s.key)
}

func (s stringSorter) Swap(a, b int) {
	s.records[a], s.records[b] = s.records[b], s.records[a]
}
