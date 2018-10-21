package rdb

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestNewReader(t *testing.T) {
	tests := []struct {
		Redis    []byte
		Expected error
	}{
		{
			Redis: []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x38, 0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
				0x2D, 0x76, 0x65, 0x72, 0x06, 0x34, 0x2E, 0x30, 0x2E, 0x31, 0x31, 0xFA, 0x0A, 0x72, 0x65, 0x64,
				0x69, 0x73, 0x2D, 0x62, 0x69, 0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65,
				0xC2, 0x8F, 0xE2, 0x8C, 0x5B, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D, 0x65, 0x6D, 0xC2,
				0x18, 0x00, 0x0C, 0x00, 0xFA, 0x0C, 0x61, 0x6F, 0x66, 0x2D, 0x70, 0x72, 0x65, 0x61, 0x6D, 0x62,
				0x6C, 0x65, 0xC0, 0x00, 0xFF, 0x1C, 0x2A, 0x76, 0xC3, 0xE9, 0xF5, 0x2A, 0x6A,
			},
			Expected: nil,
		},
		{
			Redis: []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x38,
			},
			Expected: io.EOF,
		},
		{
			Redis: []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x38, 0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
			},
			Expected: io.EOF,
		},
		{
			Redis:    []byte{},
			Expected: io.EOF,
		},
		{
			Redis: []byte{
				0x52, 0x45, 0x44,
			},
			Expected: ErrFormat,
		},
		{
			Redis: []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x00, 0x32, 0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
				0x2D, 0x76, 0x65, 0x72, 0x06, 0x34, 0x2E, 0x30, 0x2E, 0x31, 0x31, 0xFA, 0x0A, 0x72, 0x65, 0x64,
				0x69, 0x73, 0x2D, 0x62, 0x69, 0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65,
				0xC2, 0x8F, 0xE2, 0x8C, 0x5B, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D, 0x65, 0x6D, 0xC2,
				0x18, 0x00, 0x0C, 0x00, 0xFA, 0x0C, 0x61, 0x6F, 0x66, 0x2D, 0x70, 0x72, 0x65, 0x61, 0x6D, 0x62,
				0x6C, 0x65, 0xC0, 0x00, 0xFF, 0x1C, 0x2A, 0x76, 0xC3, 0xE9, 0xF5, 0x2A, 0x6A,
			},
			Expected: ErrFormat,
		},
		{
			Redis: []byte{
				0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x30, 0x32, 0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
				0x2D, 0x76, 0x65, 0x72, 0x06, 0x34, 0x2E, 0x30, 0x2E, 0x31, 0x31, 0xFA, 0x0A, 0x72, 0x65, 0x64,
				0x69, 0x73, 0x2D, 0x62, 0x69, 0x74, 0x73, 0xC0, 0x40, 0xFA, 0x05, 0x63, 0x74, 0x69, 0x6D, 0x65,
				0xC2, 0x8F, 0xE2, 0x8C, 0x5B, 0xFA, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2D, 0x6D, 0x65, 0x6D, 0xC2,
				0x18, 0x00, 0x0C, 0x00, 0xFA, 0x0C, 0x61, 0x6F, 0x66, 0x2D, 0x70, 0x72, 0x65, 0x61, 0x6D, 0x62,
				0x6C, 0x65, 0xC0, 0x00, 0xFF, 0x1C, 0x2A, 0x76, 0xC3, 0xE9, 0xF5, 0x2A, 0x6A,
			},
			Expected: ErrVersion,
		},
	}

	for _, tt := range tests {
		if _, err := NewReader(bytes.NewReader(tt.Redis)); err != tt.Expected {
			t.Errorf("Expected '%v' got '%v'", tt.Expected, err)
		}
	}
}

func TestReadMetadata(t *testing.T) {
	tests := []struct {
		buffer        []byte
		expectedValue map[string]RedisString
		expectedErr   error
	}{
		{
			buffer: []byte{
				0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x33, 0x2E, 0x32, 0x2E,
				0x36, 0xFA, 0x0A, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x62, 0x69, 0x74, 0x73, 0xC0, 0x40, 0xFA,
				0x05, 0x63, 0x74, 0x69, 0x6D, 0x65, 0xC2, 0xB4, 0xF5, 0x88, 0x5B, 0xFA, 0x08, 0x75, 0x73, 0x65,
				0x64, 0x2D, 0x6D, 0x65, 0x6D, 0xC2, 0x08, 0x62, 0xDF, 0x38, 0xFE,
			},
			expectedValue: map[string]RedisString{
				"redis-ver":  RedisString("3.2.6"),
				"redis-bits": RedisString([]byte{0x40}),
				"ctime":      RedisString([]byte{0xB4, 0xF5, 0x88, 0x5B}),
				"used-mem":   RedisString([]byte{0x08, 0x62, 0xDF, 0x38}),
			},
			expectedErr: nil,
		},
		{
			buffer: []byte{
				0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x33, 0x2E, 0x32, 0x2E,
				0x36,
			},
			expectedValue: nil,
			expectedErr:   io.EOF,
		},
		{
			buffer: []byte{
				0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05, 0x33, 0x2E, 0x32, 0x2E,
				0x36, 0x00,
			},
			expectedValue: nil,
			expectedErr:   ErrBadOpCode,
		},
		{
			buffer: []byte{
				0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x05,
			},
			expectedValue: nil,
			expectedErr:   io.EOF,
		},
		{
			buffer: []byte{
				0xFA,
			},
			expectedValue: nil,
			expectedErr:   io.EOF,
		},
	}

	for _, tt := range tests {
		md, err := readMetadata(bufio.NewReader(bytes.NewReader(tt.buffer)))
		if !reflect.DeepEqual(tt.expectedValue, md) {
			t.Errorf("Expected '%v' got '%v'", tt.expectedValue, md)
		}
		if tt.expectedErr != err {
			t.Errorf("Expected '%v' got '%v'", tt.expectedErr, err)
		}
	}
}

func TestSetDBNo(t *testing.T) {
	initialDbNo := uint64(5)
	tests := []struct {
		buffer                []byte
		expectedValue         uint64
		expectedErr           error
		nextByteExpectedValue []byte
		nextByteExpectedErr   error
	}{
		{
			buffer:                []byte{},
			expectedValue:         initialDbNo,
			expectedErr:           io.EOF,
			nextByteExpectedValue: []byte{},
			nextByteExpectedErr:   io.EOF,
		},
		{
			buffer:                []byte{0xFE, 0x01, 0xFF},
			expectedValue:         1,
			expectedErr:           nil,
			nextByteExpectedValue: []byte{0xFF},
			nextByteExpectedErr:   nil,
		},
		{
			buffer:                []byte{0xFE, 0x01},
			expectedValue:         initialDbNo,
			expectedErr:           io.EOF,
			nextByteExpectedValue: []byte{},
			nextByteExpectedErr:   io.EOF,
		},
		{
			buffer:                []byte{0xFE, 0x01, 0xFB, 0x02, 0x03, 0xFF},
			expectedValue:         1,
			expectedErr:           nil,
			nextByteExpectedValue: []byte{0xFF},
			nextByteExpectedErr:   nil,
		},
		{
			buffer:                []byte{0xFE, 0x01, 0xFB},
			expectedValue:         initialDbNo,
			expectedErr:           io.EOF,
			nextByteExpectedValue: []byte{},
			nextByteExpectedErr:   io.EOF,
		},
		{
			buffer:                []byte{0xFE, 0x01, 0xFB, 0x02},
			expectedValue:         initialDbNo,
			expectedErr:           io.EOF,
			nextByteExpectedValue: []byte{},
			nextByteExpectedErr:   io.EOF,
		},
		{
			buffer:                []byte{0xFA},
			expectedValue:         initialDbNo,
			expectedErr:           ErrBadOpCode,
			nextByteExpectedValue: []byte{},
			nextByteExpectedErr:   io.EOF,
		},
		{
			buffer:                []byte{0xFE},
			expectedValue:         initialDbNo,
			expectedErr:           io.EOF,
			nextByteExpectedValue: []byte{},
			nextByteExpectedErr:   io.EOF,
		},
	}

	for _, tt := range tests {
		r := &Reader{
			buffer: bufio.NewReader(bytes.NewReader(tt.buffer)),
			dbno:   initialDbNo,
		}
		err := setDBNo(r)
		if err != tt.expectedErr {
			t.Errorf("Expected '%v' got '%v'", tt.expectedErr, err)
		}
		if r.dbno != tt.expectedValue {
			t.Errorf("Expected '%v' got '%v'", tt.expectedValue, r.dbno)
		}
		if b, err := r.buffer.Peek(1); err != tt.nextByteExpectedErr && bytes.Equal(b, tt.nextByteExpectedValue) {
			t.Errorf("Expected '%v, %v', got '%v, %v'", tt.nextByteExpectedValue, tt.nextByteExpectedErr, b, err)
		}
	}
}

func TestReadListEncodedValue(t *testing.T) {
	tests := []struct {
		buffer        []byte
		expectedValue []RedisString
		expectedRaw   []byte
		expectedErr   error
	}{
		{
			buffer:        []byte{0x02, 0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x06, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x21},
			expectedValue: []RedisString{RedisString("Hello"), RedisString("world!")},
			expectedRaw:   []byte{0x02, 0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x06, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x21},
			expectedErr:   nil,
		},
		{
			buffer:        []byte{0x02, 0x05, 0x48, 0x65, 0x6C, 0x6C, 0x6F},
			expectedValue: nil,
			expectedRaw:   nil,
			expectedErr:   io.EOF,
		},
		{
			buffer:        []byte{},
			expectedValue: nil,
			expectedRaw:   nil,
			expectedErr:   io.EOF,
		},
	}

	for _, tt := range tests {
		list, raw, err := readListEncodedValue(bufio.NewReader(bytes.NewReader(tt.buffer)))
		if !reflect.DeepEqual(tt.expectedValue, list) {
			t.Errorf("Expected '%v' got '%v'", tt.expectedValue, list)
		}
		if !bytes.Equal(tt.expectedRaw, raw) {
			t.Errorf("Expected '%v' got '%v'", tt.expectedRaw, raw)
		}
		if tt.expectedErr != err {
			t.Errorf("Expected '%v' got '%v'", tt.expectedErr, err)
		}
	}
}

func TestReadLenghtEncodedValue(t *testing.T) {
	tests := []struct {
		Buffer        []byte
		ExpectedValue uint64
		ExpectedRaw   []byte
		ExpectedErr   error
	}{
		{
			Buffer:        []byte{0x05},
			ExpectedValue: 5,
			ExpectedRaw:   []byte{0x05},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0x42, 0xFF},
			ExpectedValue: 767,
			ExpectedRaw:   []byte{0x42, 0xFF},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0x80, 0x42, 0x31, 0x20, 0x53},
			ExpectedValue: 1110515795,
			ExpectedRaw:   []byte{0x80, 0x42, 0x31, 0x20, 0x53},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0x80, 0x42, 0x31, 0x20},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   ErrFormat,
		},
		{
			Buffer:        []byte{0x80},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   io.EOF,
		},
		{
			Buffer:        []byte{0x81, 0x01, 0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78},
			ExpectedValue: 77162851027281784,
			ExpectedRaw:   []byte{0x81, 0x01, 0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0x81, 0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   ErrFormat,
		},
		{
			Buffer:        []byte{0x82, 0x01, 0x12, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   ErrFormat,
		},
		{
			Buffer:        []byte{0xC0},
			ExpectedValue: 1,
			ExpectedRaw:   []byte{0xC0},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0xC1},
			ExpectedValue: 2,
			ExpectedRaw:   []byte{0xC1},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0xC2},
			ExpectedValue: 4,
			ExpectedRaw:   []byte{0xC2},
			ExpectedErr:   nil,
		},
		{
			Buffer:        []byte{0xC3, 0x01, 0x02, 0x03, 0x04},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   ErrNotSupported,
		},
		{
			Buffer:        []byte{0xFF},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   ErrFormat,
		},
		{
			Buffer:        []byte{0x42},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   io.EOF,
		},
		{
			Buffer:        []byte{},
			ExpectedValue: 0,
			ExpectedRaw:   nil,
			ExpectedErr:   io.EOF,
		},
	}

	for i, tt := range tests {
		bs, raw, err := readLenghtEncodedValue(bufio.NewReader(bytes.NewReader(tt.Buffer)))
		if tt.ExpectedValue != bs {
			t.Errorf("Failed '%d': Expected '%v' got '%v'", i, tt.ExpectedValue, bs)
		}
		if !bytes.Equal(tt.ExpectedRaw, raw) {
			t.Errorf("Failed '%d': Expected '%v' got '%v'", i, tt.ExpectedRaw, raw)
		}
		if tt.ExpectedErr != err {
			t.Errorf("Failed '%d': Expected '%v' got '%v'", i, tt.ExpectedErr, err)
		}
	}
}

func TestReadStringEncodedValue(t *testing.T) {
	tests := []struct {
		buffer        []byte
		expectedValue RedisString
		expectedRaw   []byte
		expectedErr   error
	}{
		{
			buffer:        []byte{0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x21},
			expectedValue: RedisString("Hello, world!"),
			expectedRaw:   []byte{0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64, 0x21},
			expectedErr:   nil,
		},
		{
			buffer:        []byte{0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64},
			expectedValue: RedisString(""),
			expectedRaw:   nil,
			expectedErr:   io.EOF,
		},
		{
			buffer:        []byte{0x01},
			expectedValue: RedisString(""),
			expectedRaw:   nil,
			expectedErr:   io.EOF,
		},
		{
			buffer:        []byte{},
			expectedValue: RedisString(""),
			expectedRaw:   nil,
			expectedErr:   io.EOF,
		},
	}

	for _, tt := range tests {
		rs, raw, err := readStringEncodedValue(bufio.NewReader(bytes.NewReader(tt.buffer)))
		if !bytes.Equal(tt.expectedValue, rs) {
			t.Errorf("Expected '%v' got '%v'", tt.expectedValue, rs)
		}
		if !bytes.Equal(tt.expectedRaw, raw) {
			t.Errorf("Expected '%v' got '%v'", tt.expectedRaw, raw)
		}
		if tt.expectedErr != err {
			t.Errorf("Expected '%v' got '%v'", tt.expectedErr, err)
		}
	}
}

func TestPad(t *testing.T) {
	tests := []struct {
		in   []byte
		size int
		out  []byte
	}{
		{
			in:   []byte{},
			size: 8,
			out:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			in:   []byte{1},
			size: 4,
			out:  []byte{0, 0, 0, 1},
		},
		{
			in:   []byte{1, 2, 3, 0, 0},
			size: 8,
			out:  []byte{0, 0, 0, 1, 2, 3, 0, 0},
		},
	}

	for _, tt := range tests {
		bs := pad(tt.in, tt.size)
		if !bytes.Equal(tt.out, bs) {
			t.Errorf("Expected '%v' got '%v'", tt.out, bs)
		}
	}
}
