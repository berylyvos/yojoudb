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
}
