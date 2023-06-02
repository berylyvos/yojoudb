package fio

const (
	DataFilePerm = 0644
)

type FileIOType = byte

const (
	IOStdFile FileIOType = iota
	IOMMap
)

// IOManager abstract file IO manager
type IOManager interface {
	// Read reads data by the given location of a file
	Read([]byte, int64) (int, error)

	// Write writes data to a file
	Write([]byte) (int, error)

	// Sync persists file data into disk
	Sync() error

	// Close closes a file
	Close() error

	// Size gets size of a file
	Size() (int64, error)
}

func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case IOStdFile:
		return NewFileIOManager(fileName)
	case IOMMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported io type")
	}
}
