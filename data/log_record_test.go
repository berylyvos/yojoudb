package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	record1 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("yojou"),
		Type: LRNormal,
	}
	buf1, size := Encode(record1)
	t.Log(buf1, size)
	assert.NotNil(t, buf1)
	assert.Greater(t, size, int64(5))

	// value is nil
	record2 := &LogRecord{
		Key:  []byte("name"),
		Type: LRNormal,
	}
	buf2, size2 := Encode(record2)
	t.Log(buf2, size2)
	assert.NotNil(t, buf2)
	assert.Greater(t, size2, int64(5))

	// Deleted type
	record3 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("yojou"),
		Type: LRDeleted,
	}
	buf3, size3 := Encode(record3)
	t.Log(buf3, size3)
	assert.NotNil(t, buf3)
	assert.Greater(t, size3, int64(5))
}

func TestDecodeLogRecord(t *testing.T) {
	headerBuf := []byte{250, 39, 65, 252, 0, 8, 10}
	header, size := decodeLRHeader(headerBuf)
	assert.NotNil(t, header)
	assert.Equal(t, int64(7), size)
	assert.Equal(t, uint32(4232128506), header.crc)
	assert.Equal(t, LRNormal, header.typ)
	assert.Equal(t, uint32(4), header.ksz)
	assert.Equal(t, uint32(5), header.vsz)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	header2, size2 := decodeLRHeader(headerBuf2)
	assert.NotNil(t, header2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), header2.crc)
	assert.Equal(t, LRNormal, header2.typ)
	assert.Equal(t, uint32(4), header2.ksz)
	assert.Equal(t, uint32(0), header2.vsz)

	headerBuf3 := []byte{149, 107, 228, 103, 1, 8, 10}
	header3, size3 := decodeLRHeader(headerBuf3)
	assert.NotNil(t, header3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(1743022997), header3.crc)
	assert.Equal(t, LRDeleted, header3.typ)
	assert.Equal(t, uint32(4), header3.ksz)
	assert.Equal(t, uint32(5), header3.vsz)
}
