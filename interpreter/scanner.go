package interpreter

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

type TokenType int

const (
	EOF TokenType = iota

	// Errors
	ReadError
	UnknownToken
	InvalidString

	// Keywords
	Select
	From
	Where
	Group
	By

	// Types
	Identifier
	String
	Integer
	Boolean

	// Punctuation
	Period
	Comma
	OpenParen
	CloseParen
)

type Token struct {
	Type TokenType
	Val  interface{}
	Char int
	Line int
}

type Scanner struct {
	reader  *bufio.Reader
	buf     []rune
	current rune
	hold    bool
	err     error
	char    int
	line    int
}

func NewScanner(reader *bufio.Reader) *Scanner {
	return &Scanner{
		reader: reader,
		buf:    make([]rune, 0, 1024),
		char:   -1,
	}
}

func (s *Scanner) Next() Token {
	s.clear()

	current := s.read()
	for isSpace(current) {
		s.discard()
		current = s.read()
	}

	// Singles
	switch current {
	case '.':
		return s.readSingle(Period)
	case ',':
		return s.readSingle(Comma)
	case '(':
		return s.readSingle(OpenParen)
	case ')':
		return s.readSingle(CloseParen)
	}

	// Errors
	if s.err == io.EOF {
		return Token{
			Type: EOF,
			Line: s.line,
			Char: s.char,
		}
	} else if s.err != nil {
		return Token{
			Type: ReadError,
			Val:  s.err,
			Line: s.line,
			Char: s.char,
		}
	}

	if isAlpha(current) {
		return s.readWord()
	} else if isNumeric(current) || current == '-' {
		return s.readInteger()
	} else if current == '"' {
		return s.readString()
	}

	char, line := s.char, s.line
	s.append()
	return Token{
		Type: UnknownToken,
		Val:  string(s.buf),
		Line: line,
		Char: char,
	}
}

func (s *Scanner) read() rune {
	if s.err != nil || s.hold {
		return s.current
	}

	s.current, _, s.err = s.reader.ReadRune()
	s.hold = true
	if s.err != nil {
		s.current = utf8.RuneError
	}

	if s.current == '\n' {
		s.line++
		s.char = 0
	} else {
		s.char++
	}

	return s.current
}

func (s *Scanner) readSingle(Type TokenType) Token {
	char, line := s.char, s.line
	s.append()
	return Token{
		Type: Type,
		Val:  string(s.buf),
		Char: char,
		Line: line,
	}
}

func (s *Scanner) readWord() Token {
	char, line := s.char, s.line
	for {
		current := s.read()
		if !isAlpha(current) && !isNumeric(current) {
			break
		}
		s.append()
	}

	word := string(s.buf)
	result := Token{
		Char: char,
		Line: line,
	}

	switch strings.ToUpper(word) {
	case "SELECT":
		result.Type = Select
		result.Val = word
	case "FROM":
		result.Type = From
		result.Val = word
	case "WHERE":
		result.Type = Where
		result.Val = word
	case "GROUP":
		result.Type = Group
		result.Val = word
	case "BY":
		result.Type = By
		result.Val = word
	case "TRUE":
		result.Type = Boolean
		result.Val = true
	case "FALSE":
		result.Type = Boolean
		result.Val = false
	default:
		result.Type = Identifier
		result.Val = word
	}

	return result
}

func (s *Scanner) readInteger() Token {
	current := s.read()
	char, line := s.char, s.line
	s.append()

	for {
		current = s.read()
		if !isNumeric(current) {
			break
		}
		s.append()
	}

	intStr := string(s.buf)
	parsed, err := strconv.Atoi(intStr)
	if err != nil {
		return Token{
			Type: UnknownToken,
			Val:  intStr,
			Char: char,
			Line: line,
		}
	}

	return Token{
		Type: Integer,
		Val:  parsed,
		Char: char,
		Line: line,
	}
}

func (s *Scanner) readString() Token {
	current := s.read()
	char, line := s.char, s.line
	s.discard()

	for {
		current = s.read()
		if current == '"' {
			s.discard()
			return Token{
				Type: String,
				Val:  string(s.buf),
				Char: char,
				Line: line,
			}
		} else if current == '\\' {
			s.discard()
			if current = s.read(); current == '\\' || current == '"' {
				s.append()
			} else if current == 'n' {
				s.discard()
				s.push('\n')
			} else {
				return Token{
					Type: InvalidString,
					Val:  string(s.buf),
					Char: char,
					Line: line,
				}
			}
		} else {
			s.append()
		}
	}
}

func (s *Scanner) clear() {
	s.buf = s.buf[:0]
}

func (s *Scanner) append() {
	s.buf = append(s.buf, s.current)
	s.hold = false
}

func (s *Scanner) discard() {
	s.hold = false
}

func (s *Scanner) push(r rune) {
	s.buf = append(s.buf, r)
}

func isSpace(r rune) bool {
	return r == ' ' || r == '\r' || r == '\n' || r == '\t'
}

func isAlpha(r rune) bool {
	return r == '_' || ('a' <= r && r <= 'z') || ('A' <= r && r <= 'Z')
}

func isNumeric(r rune) bool {
	return '0' <= r && r <= '9'
}
