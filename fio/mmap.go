package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap uses mmap.ReaderAt for reading files
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{
		readerAt: readerAt,
	}, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(b, offset)
}

func (m *MMap) Write(bytes []byte) (int, error) {
	panic("implement me")
}

func (m *MMap) Sync() error {
	panic("implement me")
}

func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
