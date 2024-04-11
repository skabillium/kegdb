package keg

import (
	"encoding/binary"
	"hash/crc32"
)

const HeaderLength = 4 * 4

type Header struct {
	Checksum  uint32
	Timestamp uint32
	KeySize   uint32
	ValueSize uint32
}

func (h *Header) Encode() []byte {
	buf := make([]byte, HeaderLength)

	binary.LittleEndian.PutUint32(buf, h.Checksum)
	binary.LittleEndian.PutUint32(buf, h.Timestamp)
	binary.LittleEndian.PutUint32(buf, h.KeySize)
	binary.LittleEndian.PutUint32(buf, h.ValueSize)

	return buf
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
	buf := r.Header.Encode()
	buf = append(buf, []byte(r.Key)...)
	buf = append(buf, r.Value...)

	return buf, nil
}
