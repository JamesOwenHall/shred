package interpreter

import (
	"bufio"
	"strings"
	"testing"
)

func TestScannerPunctuation(t *testing.T) {
	input := bufio.NewReader(strings.NewReader(".,()"))
	expected := []TokenType{Period, Comma, OpenParen, CloseParen}

	scanner := NewScanner(input)
	for i, expected := range expected {
		actual := scanner.Next()
		if actual.Type != expected {
			t.Fatalf("#%d\nexpected: %v\n  actual: %v", i, expected, actual)
		}
	}

	if actual := scanner.Next(); actual.Type != EOF {
		t.Fatalf("\nexpected: EOF\n  actual: %v", actual)
	}
}

func TestScannerKeywords(t *testing.T) {
	input := bufio.NewReader(strings.NewReader("SELECT FROM WHERE GROUP BY"))
	expected := []TokenType{Select, From, Where, Group, By}

	scanner := NewScanner(input)
	for i, expected := range expected {
		actual := scanner.Next()
		if actual.Type != expected {
			t.Fatalf("#%d\nexpected: %v\n  actual: %v", i, expected, actual)
		}
	}

	if actual := scanner.Next(); actual.Type != EOF {
		t.Fatalf("\nexpected: EOF\n  actual: %v", actual)
	}
}

func TestScannerIdentifier(t *testing.T) {
	input := bufio.NewReader(strings.NewReader("foo BAR _baz321"))
	expected := []string{"foo", "BAR", "_baz321"}

	scanner := NewScanner(input)
	for i, expected := range expected {
		actual := scanner.Next()
		if actual.Type != Identifier || actual.Val.(string) != expected {
			t.Fatalf("#%d\nexpected: %v\n  actual: %v", i, expected, actual)
		}
	}

	if actual := scanner.Next(); actual.Type != EOF {
		t.Fatalf("\nexpected: EOF\n  actual: %v", actual)
	}
}

func TestScannerInteger(t *testing.T) {
	input := bufio.NewReader(strings.NewReader("123 -9 0"))
	expected := []int{123, -9, 0}

	scanner := NewScanner(input)
	for i, expected := range expected {
		actual := scanner.Next()
		if actual.Type != Integer || actual.Val.(int) != expected {
			t.Fatalf("#%d\nexpected: %v\n  actual: %v", i, expected, actual)
		}
	}

	if actual := scanner.Next(); actual.Type != EOF {
		t.Fatalf("\nexpected: EOF\n  actual: %v", actual)
	}
}

func TestScannerString(t *testing.T) {
	input := bufio.NewReader(strings.NewReader(`"foo""\\\"\n"`))
	expected := []string{"foo", "\\\"\n"}

	scanner := NewScanner(input)
	for i, expected := range expected {
		actual := scanner.Next()
		if actual.Type != String || actual.Val.(string) != expected {
			t.Fatalf("#%d\nexpected: %v\n  actual: %v", i, expected, actual)
		}
	}

	if actual := scanner.Next(); actual.Type != EOF {
		t.Fatalf("\nexpected: EOF\n  actual: %v", actual)
	}
}

func TestScannerBoolean(t *testing.T) {
	input := bufio.NewReader(strings.NewReader(`false true`))
	expected := []bool{false, true}

	scanner := NewScanner(input)
	for i, expected := range expected {
		actual := scanner.Next()
		if actual.Type != Boolean || actual.Val.(bool) != expected {
			t.Fatalf("#%d\nexpected: %v\n  actual: %v", i, expected, actual)
		}
	}

	if actual := scanner.Next(); actual.Type != EOF {
		t.Fatalf("\nexpected: EOF\n  actual: %v", actual)
	}
}
