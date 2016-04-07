package shred

import (
	"reflect"
	"testing"
)

func TestDatasetCollect(t *testing.T) {
	input := &RecordIterator{
		{"foo": 1},
		{"foo": 2},
		{"foo": 3},
	}
	expected := []Record{
		{"foo": 1},
		{"foo": 2},
		{"foo": 3},
	}

	actual, err := NewDataset(input).Collect()

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected: %v\nactual: %v", expected, actual)
	}
}

func TestDatasetClone(t *testing.T) {
	a := NewDataset(&RecordIterator{
		{"foo": 1},
		{"foo": 2},
		{"foo": 3},
	}).Reduce(func(a, b Record) Record {
		return a.Set("foo", a.Int("foo")+b.Int("foo"))
	})
	b := a.Clone().(*Dataset)

	aActual, err := a.Collect()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(aActual, []Record{{"foo": 6}}) {
		t.Fatalf("unexpected: %v", aActual)
	}

	bActual, err := b.Collect()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bActual, []Record{{"foo": 6}}) {
		t.Fatalf("unexpected: %v", bActual)
	}
}

func TestDatasetFilter(t *testing.T) {
	input := &RecordIterator{
		{"foo": 1},
		{"foo": 2},
		{"foo": 3},
	}
	expected := []Record{
		{"foo": 1},
		{"foo": 3},
	}

	actual, err := NewDataset(input).Filter(func(r Record) bool {
		return r.Int("foo")%2 == 1
	}).Collect()

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected: %v\nactual: %v", expected, actual)
	}
}

func TestDatasetMap(t *testing.T) {
	input := &RecordIterator{
		{"foo": 1},
		{"foo": 2},
		{"foo": 3},
	}
	expected := []Record{
		{"foo": 2},
		{"foo": 4},
		{"foo": 6},
	}

	actual, err := NewDataset(input).Map(func(r Record) Record {
		return r.Set("foo", 2*r.Int("foo"))
	}).Collect()

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected: %v\nactual: %v", expected, actual)
	}
}

func TestDatasetReduce(t *testing.T) {
	input := &RecordIterator{
		{"foo": 1},
		{"foo": 2},
		{"foo": 3},
	}
	expected := []Record{
		{"foo": 6},
	}

	actual, err := NewDataset(input).Reduce(func(a, b Record) Record {
		return a.Set("foo", a.Int("foo")+b.Int("foo"))
	}).Collect()

	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected: %v\nactual: %v", expected, actual)
	}
}

func TestDatasetErrorPropagation(t *testing.T) {
	input := new(FailingIterator)

	actual, err := NewDataset(input).Filter(func(_ Record) bool {
		return true
	}).Map(func(r Record) Record {
		return r
	}).Reduce(func(a, b Record) Record {
		return a
	}).Collect()

	if err == nil {
		t.Fatalf("unexpected nil error, actual: %v", actual)
	} else if err != ErrFailingIterator {
		t.Fatalf("unexpected error: %v", err)
	}
}
