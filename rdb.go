package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	minVersion = 3
	maxVersion = 8
)

// ValueType ...
type ValueType int

const (
	// STRING ...
	STRING ValueType = iota
	// LIST ...
	LIST
	// SET ...
	SET
	// ZSET ...
	ZSET
	// HASH ...
	HASH
	// ZSET2 is ZSET version 2 with doubles stored in binary.
	ZSET2
	// MODULE ...
	MODULE
	// MODULE2 ...
	MODULE2
	// HASHZIPMAP ...
	HASHZIPMAP
	// LISTZIPLIST ...
	LISTZIPLIST
	// SETINTSET ...
	SETINTSET
	// ZSETZIPLLIST ...
	ZSETZIPLLIST
	// HASHZIPLIST ...
	HASHZIPLIST
	// LISTQUICKLIST ...
	LISTQUICKLIST
	// STREAMLISTPACKS ...
	STREAMLISTPACKS
)

// RedisString ...
type RedisString []byte

var (
	// ErrFormat ...
	ErrFormat = errors.New("Not an RDB file")
	// ErrVersion ...
	ErrVersion = errors.New("Unsupported version")
	// ErrNotSupported ...
	ErrNotSupported = errors.New("Unsupported feature")
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

	buf := make([]byte, 5)
	if _, err := buffer.Read(buf); err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte("REDIS"), buf) {
		return nil, ErrFormat
	}

	buf = make([]byte, 4)
	buffer.Read(buf)

	v, err := strconv.Atoi(string(buf))
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

// Read ...
func (r *Reader) Read() (uint64, uint64, ValueType, RedisString, []byte, error) {
	b, err := r.buffer.Peek(1)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	if bytes.Equal(b, []byte{0xFE}) {
		if err := setDBNo(r); err != nil {
			return 0, 0, 0, nil, nil, err
		}
	}
	return 0, 0, 0, nil, nil, nil
}

func readFieldLength(r *bufio.Reader) (uint64, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch b >> 6 {
	case 0: // 6 bit integer
		return binary.BigEndian.Uint64(pad([]byte{b << 2 >> 2}, 8)), nil
	case 1: // 14 bit integer
		b2, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return binary.BigEndian.Uint64(pad([]byte{b << 2 >> 2, b2}, 8)), nil
	case 2:
		var nb int // Numbe of bytes to read
		switch b << 2 >> 2 {
		case 0: // 32 bit integer
			nb = 4
		case 1: // 64 bit integer
			nb = 8
		default: // 64 bit integer
			return 0, ErrFormat
		}
		bs := make([]byte, nb)
		n, err := r.Read(bs)
		if err != nil {
			return 0, err
		}
		if n < nb {
			return 0, ErrFormat
		}
		return binary.BigEndian.Uint64(pad(bs, 8)), nil
	case 3: //String encoded field
		var nb int // Numbe of bytes to read
		switch b << 2 >> 2 {
		case 0:
			nb = 1
		case 1:
			nb = 2
		case 2:
			nb = 4
		case 3:
			return 0, ErrNotSupported
		default:
			return 0, ErrFormat
		}
		bs := make([]byte, nb)
		n, err := r.Read(bs)
		if err != nil {
			return 0, err
		}
		if n < nb {
			return 0, ErrFormat
		}
		return binary.BigEndian.Uint64(pad(bs, 8)), nil
	default:
		panic("The universe is broken!") // To satisfy compiler
	}
}

func setDBNo(r *Reader) error {
	b, err := r.buffer.Peek(1)
	if err != nil {
		return err
	}
	if !bytes.Equal(b, []byte{0xFE}) {
		return fmt.Errorf("Not DB Selector")
	}
	r.buffer.Discard(1)
	// r.source.Read()
	return nil
}

func pad(bs []byte, size int) []byte {
	final := make([]byte, size)
	offset := size - len(bs)
	for i := 0; i < len(bs); i++ {
		final[offset+i] = bs[i]
	}
	return final
}
