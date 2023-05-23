package data

import (
	"fmt"
	"path/filepath"
	"yojoudb/fio"
)

const (
	DataFileSuffix = ".ddd"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

func OpenDataFile(dir string, fid uint32) (*DataFile, error) {
	fileName := GetDataFileName(dir, fid)
	return newDataFile(fileName, fid)
}

func GetDataFileName(dir string, fid uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%09d", fid)+DataFileSuffix)
}

func newDataFile(dir string, fid uint32) (*DataFile, error) {
	fileIO, err := fio.NewFileIOManager(dir)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fid,
		WriteOff:  0,
		IoManager: fileIO,
	}, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}

func (df *DataFile) Write(b []byte) error {
	sz, err := df.IoManager.Write(b)
	if err != nil {
		return err
	}
	df.WriteOff += int64(sz)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}
