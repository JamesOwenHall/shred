package shred

import (
	"reflect"
	"testing"
)

func TestRecordAccess(t *testing.T) {
	rec := Record{
		"int":    5,
		"string": "foo",
	}

	// Get
	if actual := rec.Get("int"); !reflect.DeepEqual(actual, 5) {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.Get("string"); !reflect.DeepEqual(actual, "foo") {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.Get("non-existent"); !reflect.DeepEqual(actual, nil) {
		t.Fatalf("unexpected: %v", actual)
	}

	if actual := rec.GetOr("string", "bar"); !reflect.DeepEqual(actual, "foo") {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.GetOr("non-existent", "bar"); !reflect.DeepEqual(actual, "bar") {
		t.Fatalf("unexpected: %v", actual)
	}

	// Int
	if actual := rec.Int("int"); actual != 5 {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.IntOr("int", 42); actual != 5 {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.Int("string"); actual != 0 {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.IntOr("string", 42); actual != 42 {
		t.Fatalf("unexpected: %v", actual)
	}

	// String
	if actual := rec.String("string"); actual != "foo" {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.StringOr("string", "bar"); actual != "foo" {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.String("int"); actual != "" {
		t.Fatalf("unexpected: %v", actual)
	}
	if actual := rec.StringOr("int", "bar"); actual != "bar" {
		t.Fatalf("unexpected: %v", actual)
	}
}

func TestRecordSetClone(t *testing.T) {
	a := Record{
		"foo": "bar",
	}
	b := a.Set("foo", "baz")

	if a["foo"] != "bar" {
		t.Fatalf("unexpected: %v", a["foo"])
	}
	if b["foo"] != "baz" {
		t.Fatalf("unexpected: %v", b["foo"])
	}
}

func TestRecordMerge(t *testing.T) {
	a := Record{
		"foo": 1,
		"bar": 2,
	}
	b := Record{
		"bar": 200,
		"baz": 300,
	}

	c := a.Merge(b)
	if c["foo"] != 1 {
		t.Fatalf("unexpected: %v", c["foo"])
	}
	if c["bar"] != 200 {
		t.Fatalf("unexpected: %v", c["bar"])
	}
	if c["baz"] != 300 {
		t.Fatalf("unexpected: %v", c["baz"])
	}
}
