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
func readFieldLength(r *bufio.Reader) ([]byte, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch b >> 6 {
	case 0: // 6 bit integer
		return []byte{b << 2 >> 2}, nil
	case 1: // 14 bit integer
		b2, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		return []byte{b << 2 >> 2, b2}, nil
	case 2:
		var nb int // Numbe of bytes to read
		switch b << 2 >> 2 {
		case 0: // 32 bit integer
			nb = 4
		case 1: // 64 bit integer
			nb = 8
		}
		bs := make([]byte, nb)
		n, err := r.Read(bs)
		if err != nil {
			return nil, err
		}
		if n < nb {
			return nil, ErrFormat
		}
		return bs, nil
	case 3: //String encoded field
		switch b << 2 >> 2 {
		case 0:
		case 1:
		case 2:
		case 4:
		default:
			return nil, ErrFormat
		}
	}
	return nil, nil
}

