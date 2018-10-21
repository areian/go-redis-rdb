package rdb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
)

const (
	minVersion = 7
	maxVersion = 8
)

// ValueType ...
type ValueType byte

const (
	// String Value identifier
	String ValueType = iota
	// List ...
	List
	// Set ...
	Set
	// Zset ...
	Zset
	// Hash ...
	Hash
	// Zset2 is ZSET version 2 with doubles stored in binary.
	Zset2
	// Module ...
	Module
	// Module2 ...
	Module2
	// HashZipmap ...
	HashZipmap
	// ListZipList ...
	ListZipList
	// SetIntSet ...
	SetIntSet
	// ZSetZipList ...
	ZSetZipList
	// HashZipList ...
	HashZipList
	// ListQuickList ...
	ListQuickList
	// StreamListPacks ...
	StreamListPacks
)

const (
	opAux          byte = 0xFA
	opResizeDB     byte = 0xFB
	opExpiretimeMs byte = 0xFC
	opExpiretime   byte = 0xFD
	opSelectDB     byte = 0xFE
	opEOF          byte = 0xFF
)

// RedisString ...
type RedisString []byte

var (
	// ErrFormat ...
	ErrFormat = errors.New("Not an RDB file")
	// ErrBadOpCode ..
	ErrBadOpCode = errors.New("Bad OP Code")
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

	b, err := buffer.Peek(1)
	if err != nil {
		return nil, err
	}
	metadata := make(map[string]RedisString)
	if b[0] == opAux {
		md, err := readMetadata(buffer)
		if err != nil {
			return nil, err
		}
		metadata = md
	}

	return &Reader{
		Version:  v,
		buffer:   buffer,
		Metadata: metadata,
	}, nil
}

// Read ... Returns dbno, ttl, ValueType, Key, value, error
func (r *Reader) Read() (uint64, uint64, ValueType, RedisString, RedisString, error) {
	b, err := r.buffer.Peek(1)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	if b[0] == opSelectDB {
		if err = setDBNo(r); err != nil {
			return 0, 0, 0, nil, nil, err
		}
	}
	ttl, vt, key, value, err := readKeyValuePair(r.buffer)
	if err != nil {
		return 0, 0, 0, nil, nil, err
	}
	return r.dbno, ttl, vt, key, value, nil
}

func readKeyValuePair(r *bufio.Reader) (uint64, ValueType, RedisString, []byte, error) {
	var ttl uint64
	var vt ValueType

	// Read TTL if available
	buf := []byte{0}
	if _, err := r.Read(buf); err != nil {
		return 0, 0, nil, nil, err
	}
	switch buf[0] {
	case opExpiretimeMs:
		t, _, err := readLenghtEncodedValue(r)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		ttl = t
	case opExpiretime:
		t, _, err := readLenghtEncodedValue(r)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		ttl = t * 1000
	default:
		r.UnreadByte()
	}

	// Read key/value
	if _, err := r.Read(buf); err != nil {
		return 0, 0, nil, nil, err
	}
	vt = ValueType(buf[0])
	key, _, err := readStringEncodedValue(r)
	if err != nil {
		return 0, 0, nil, nil, err
	}
	switch vt {
	case List:
		_, raw, err := readListEncodedValue(r)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return ttl, vt, key, raw, nil
	case Set:
		_, raw, err := readListEncodedValue(r)
		if err != nil {
			return 0, 0, nil, nil, err
		}
		return ttl, vt, key, raw, nil
	default:
		return 0, 0, nil, nil, ErrNotSupported
	}
}

func readMetadata(r *bufio.Reader) (map[string]RedisString, error) {
	metadata := map[string]RedisString{}
	for {
		buf := make([]byte, 1)
		_, err := r.Read(buf)
		if err != nil {
			return nil, err
		}
		if buf[0] == opSelectDB || buf[0] == opEOF {
			// DB seletor, we have reached the end of the metadata
			r.UnreadByte()
			return metadata, nil
		}
		if buf[0] != opAux {
			r.UnreadByte()
			return nil, ErrBadOpCode
		}

		key, _, err := readStringEncodedValue(r)
		if err != nil {
			return nil, err
		}
		val, _, err := readStringEncodedValue(r)
		if err != nil {
			return nil, err
		}
		metadata[string(key)] = val
	}
}

func setDBNo(r *Reader) error {
	buf := make([]byte, 1)
	_, err := r.buffer.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] != opSelectDB {
		r.buffer.UnreadByte()
		return ErrBadOpCode
	}
	db, _, err := readLenghtEncodedValue(r.buffer)
	if err != nil {
		return err
	}

	_, err = r.buffer.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] == opResizeDB {
		_, _, err := readLenghtEncodedValue(r.buffer)
		if err != nil {
			return err
		}
		_, _, err = readLenghtEncodedValue(r.buffer)
		if err != nil {
			return err
		}
	} else {
		r.buffer.UnreadByte()
	}

	r.dbno = db
	return nil
}

func readListEncodedValue(r *bufio.Reader) ([]RedisString, []byte, error) {
	raw := bytes.NewBuffer([]byte{})
	ll, b, err := readLenghtEncodedValue(r)
	if err != nil {
		return nil, nil, err
	}
	raw.Write(b)
	rsl := (make([]RedisString, ll))
	for i := uint64(0); i < ll; i++ {
		rs, b, err := readStringEncodedValue(r)
		if err != nil {
			return nil, nil, err
		}
		raw.Write(b)
		rsl[i] = rs
	}
	return rsl, raw.Bytes(), nil
}

func readLenghtEncodedValue(r *bufio.Reader) (uint64, []byte, error) {
	raw := bytes.NewBuffer([]byte{})
	b, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	raw.WriteByte(b)
	switch b >> 6 {
	case 0: // 6 bit integer
		return binary.BigEndian.Uint64(pad([]byte{b << 2 >> 2}, 8)), raw.Bytes(), nil
	case 1: // 14 bit integer
		b2, err := r.ReadByte()
		if err != nil {
			return 0, nil, err
		}
		raw.WriteByte(b2)
		return binary.BigEndian.Uint64(pad([]byte{b << 2 >> 2, b2}, 8)), raw.Bytes(), nil
	case 2:
		var nb int // Numbe of bytes to read
		switch b << 2 >> 2 {
		case 0: // 32 bit integer
			nb = 4
		case 1: // 64 bit integer
			nb = 8
		default: // 64 bit integer
			return 0, nil, ErrFormat
		}
		bs := make([]byte, nb)
		n, err := r.Read(bs)
		if err != nil {
			return 0, nil, err
		}
		if n < nb {
			return 0, nil, ErrFormat
		}
		raw.Write(bs)
		return binary.BigEndian.Uint64(pad(bs, 8)), raw.Bytes(), nil
	case 3: //String encoded field
		switch b << 2 >> 2 {
		case 0:
			return 1, raw.Bytes(), nil
		case 1:
			return 2, raw.Bytes(), nil
		case 2:
			return 4, raw.Bytes(), nil
		case 3:
			return 0, nil, ErrNotSupported
		default:
			return 0, nil, ErrFormat
		}
	}
	panic("The universe is broken!")
}

func readStringEncodedValue(r *bufio.Reader) (RedisString, []byte, error) {
	raw := bytes.NewBuffer([]byte{})
	l, b, err := readLenghtEncodedValue(r)
	if err != nil {
		return nil, nil, err
	}
	raw.Write(b)
	buf := make([]byte, l)
	n, err := r.Read(buf)
	if err != nil {
		return nil, nil, err
	}
	if uint64(n) < l {
		return nil, nil, io.EOF
	}
	raw.Write(buf)
	key := RedisString(buf)
	return key, raw.Bytes(), nil
}

func pad(bs []byte, size int) []byte {
	final := make([]byte, size)
	offset := size - len(bs)
	for i := 0; i < len(bs); i++ {
		final[offset+i] = bs[i]
	}
	return final
}
