package fio

const (
	DataFilePerm = 0644
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

func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
