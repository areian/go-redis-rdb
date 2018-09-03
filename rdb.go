package rdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	minVersion = 3
	maxVersion = 8
)

var (
	// ErrFormat ...
	ErrFormat = errors.New("Not an RDB file")
	// ErrVersion ...
	ErrVersion = errors.New("Unsupported version")
)

// Reader ...
type Reader struct {
	Version int
	dbno    uint64
	buffer  *bufio.Reader
}

// NewReader ...
func NewReader(r io.Reader) (*Reader, error) {
	buffer := bufio.NewReader(r)
	if p, err := buffer.Peek(5); err != nil || !bytes.Equal([]byte("REDIS"), p) {
		return nil, ErrFormat
	}
	buffer.Discard(5) // Skip peeked bytes

	vbs := make([]byte, 4)
	buffer.Read(vbs)

	v, err := strconv.Atoi(string(vbs))
	if err != nil {
		return nil, ErrFormat
	}

	if minVersion > v || v > maxVersion {
		return nil, ErrVersion
	}

	return &Reader{
		Version: v,
		buffer:  buffer,
	}, nil
}
