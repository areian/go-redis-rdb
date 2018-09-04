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
	minVersion = 7
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
	// ErrNotAuxField ..
	ErrNotAuxField = errors.New("Not Auxiliary Field")
	// ErrNotSupported ...
	ErrNotSupported = errors.New("Unsupported feature")
	// ErrVersion ...
	ErrVersion = errors.New("Unsupported version")
)

// Reader ...
type Reader struct {
	Version  int
	Metadata map[string]RedisString
	dbno     uint64
	buffer   *bufio.Reader
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
		Version:  v,
		buffer:   buffer,
		Metadata: make(map[string]RedisString),
	}, nil
}

// Read ...
func (r *Reader) Read() (uint64, uint64, ValueType, RedisString, []byte, error) {
	for {
		b, err := r.buffer.Peek(1)
		if err != nil {
			return 0, 0, 0, nil, nil, err
		}
		switch b[0] {
		case 0xFA:
			if err := setMetadata(r); err != nil {
				return 0, 0, 0, nil, nil, err
			}
		case 0xFE:
			if err := setDBNo(r); err != nil {
				return 0, 0, 0, nil, nil, err
			}
		default:
			break
		}
	}
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
		switch b << 2 >> 2 {
		case 0:
			return 1, nil
		case 1:
			return 2, nil
		case 2:
			return 4, nil
		case 3:
			return 0, ErrNotSupported
		default:
			return 0, ErrFormat
		}
	}
	panic("The universe is broken!")
}

func setMetadata(r *Reader) error {
	for {
		buf := make([]byte, 1)
		_, err := r.buffer.Read(buf)
		if err != nil {
			return err
		}
		if buf[0] == 0xFE {
			// DB seletor, we have reached the end of the metadata
			r.buffer.UnreadByte()
			return nil
		}
		if buf[0] != 0xFA {
			r.buffer.UnreadByte()
			return ErrNotAuxField
		}
		l, err := readFieldLength(r.buffer)
		if err != nil {
			return err
		}
		buf = make([]byte, l)
		_, err = r.buffer.Read(buf)
		if err != nil {
			return err
		}
		key := string(buf)

		l, err = readFieldLength(r.buffer)
		if err != nil {
			return err
		}
		buf = make([]byte, l)
		_, err = r.buffer.Read(buf)
		if err != nil {
			return err
		}
		r.Metadata[key] = RedisString(buf)
	}
}

func setDBNo(r *Reader) error {
	buf := make([]byte, 1)
	_, err := r.buffer.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] != 0xFE {
		r.buffer.UnreadByte()
		return fmt.Errorf("Not DB Selector")
	}
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
