package shred

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
