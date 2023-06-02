package data

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"yojoudb/fio"
)

const (
	CRC32Size        = 4
	DataFileSuffix   = ".ddd"
	HintFileName     = "hint"
	MergeFinFileName = "merge-fin"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

func OpenDataFile(dir string, fid uint32, ioType fio.FileIOType) (*DataFile, error) {
	fileName := GetDataFileName(dir, fid)
	return newDataFile(fileName, fid, ioType)
}

func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0, fio.IOStdFile)
}

func OpenMergeFinFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinFileName)
	return newDataFile(fileName, 0, fio.IOStdFile)
}

func GetDataFileName(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%09d", fid)+DataFileSuffix)
}

func newDataFile(dir string, fid uint32, ioType fio.FileIOType) (*DataFile, error) {
	iom, err := fio.NewIOManager(dir, ioType)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fid,
		WriteOff:  0,
		IoManager: iom,
	}, nil
}

// ReadLogRecord read a log record by the offset in data file
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fsz, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerSize int64 = maxLogRecordHeaderSize
	// check corner case
	// if there's no room to read max header size, just read until end of file
	if offset+headerSize > fsz {
		headerSize = fsz - offset
	}

	// read header
	hb, err := df.readNBytes(headerSize, offset)
	if err != nil {
		return nil, 0, err
	}

	// decode header
	h, hsz := decodeLRHeader(hb)
	if h == nil || (h.crc == 0 && h.ksz == 0 && h.vsz == 0) {
		return nil, 0, io.EOF
	}

	// set record size, type
	ksz, vsz := int64(h.ksz), int64(h.vsz)
	sz := hsz + ksz + vsz
	lr := &LogRecord{
		Type: h.typ,
	}

	// read key/value
	if ksz > 0 || vsz > 0 {
		b, err := df.readNBytes(ksz+vsz, offset+hsz)
		if err != nil {
			return nil, 0, err
		}

		lr.Key = b[:ksz]
		lr.Val = b[ksz:]
	}

	// check crc
	if calLogRecordCRC(lr, hb[CRC32Size:hsz]) != h.crc {
		return nil, 0, ErrInvalidCRC
	}

	return lr, sz, nil
}

func (df *DataFile) Write(b []byte) error {
	n, err := df.IoManager.Write(b)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) WriteHintRecord(key []byte, loc *LRLoc) error {
	lr := &LogRecord{
		Key: key,
		Val: EncodeLRLoc(loc),
	}
	encRecord, _ := Encode(lr)
	return df.Write(encRecord)
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IoManager.Read(b, offset)
	return b, err
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}
