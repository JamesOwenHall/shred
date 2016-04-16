package shred

import (
	"reflect"
	"testing"
)

func TestCsvIterator(t *testing.T) {
	input := []byte("1,John,Smith\n2,John,Smith\n")
	expected := []Record{
		{"0": "1", "1": "John", "2": "Smith"},
		{"0": "2", "1": "John", "2": "Smith"},
	}

	actual, err := NewDataset(NewCsvIterator(input)).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected: %v\nactual: %v", expected, actual)
	}
}
