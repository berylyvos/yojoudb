package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb/fio"
)

const (
	dir = "/tmp/yojoudb"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(dir, 0, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile(dir, 1, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)

	dataFile3, err := OpenDataFile(dir, 1, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestDataFile_Write(t *testing.T) {
	dataFile, err := OpenDataFile(dir, 114514, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("kv-"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte("yojoudb"))
	assert.Nil(t, err)

	err = dataFile.Write([]byte(",----*&^%$#@114-514"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile(dir, 0, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(dir, 0, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(dir, 1, fio.IOStdFile)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	record1 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("yojou"),
		Type: LRNormal,
	}
	buf1, size := Encode(record1)
	err = dataFile.Write(buf1)
	assert.Nil(t, err)
	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, size, readSize1)
	assert.Equal(t, record1, readRec1)

	record2 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("yojoudb"),
		Type: LRNormal,
	}
	buf2, size2 := Encode(record2)
	err = dataFile.Write(buf2)
	assert.Nil(t, err)
	readRec2, readSize2, err := dataFile.ReadLogRecord(size)
	assert.Equal(t, size2, readSize2)
	assert.Equal(t, record2, readRec2)

	record3 := &LogRecord{
		Key:  []byte("name"),
		Val:  []byte("删除"),
		Type: LRNormal,
	}
	buf3, size3 := Encode(record3)
	err = dataFile.Write(buf3)
	assert.Nil(t, err)
	readRec3, readSize3, err := dataFile.ReadLogRecord(size + size2)
	assert.Equal(t, size3, readSize3)
	assert.Equal(t, record3, readRec3)
}
