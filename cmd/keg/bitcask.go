package keg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const HeaderLength = 4*4 + 1 // Booleans are encoded as a single byte

var ErrInvalidChecksum = errors.New("ERR record has invalid checksum")

type Header struct {
	Checksum  uint32
	Timestamp uint32
	IsDeleted bool
	KeySize   uint32
	ValueSize uint32
}

func (h *Header) Encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

func DecodeHeader(b []byte) *Header {
	header := &Header{}
	binary.Read(bytes.NewReader(b), binary.LittleEndian, header)

	return header
}

type Record struct {
	Header Header
	Key    string
	Value  []byte
}

func NewRecord(key string, value []byte, timestamp uint32) *Record {
	return &Record{
		Header: Header{
			Checksum:  crc32.ChecksumIEEE(value),
			Timestamp: timestamp,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
		},
		Key:   key,
		Value: value,
	}
}

func (r *Record) Encode() ([]byte, error) {
	var buf bytes.Buffer
	err := r.Header.Encode(&buf)
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, binary.LittleEndian, []byte(r.Key))
	if err != nil {
		return nil, err
	}

	err = binary.Write(&buf, binary.LittleEndian, r.Value)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func DecodeRecord(r io.Reader) (*Record, error) {
	headerBytes := make([]byte, HeaderLength)
	_, err := r.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	header := DecodeHeader(headerBytes)
	keyBytes := make([]byte, int(header.KeySize))
	_, err = r.Read(keyBytes)
	if err != nil {
		return nil, err
	}

	valueBytes := make([]byte, int(header.ValueSize))
	_, err = r.Read(valueBytes)
	if err != nil {
		return nil, err
	}

	record := NewRecord(string(keyBytes), valueBytes, header.Timestamp)
	if record.Header.Checksum != header.Checksum {
		return nil, ErrInvalidChecksum
	}

	return record, nil
}
