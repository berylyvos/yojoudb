package data

type LRType = byte

const (
	LRNormal LRType = iota
	LRDeleted
)

// LogRecord a record in a file
type LogRecord struct {
	Key  []byte
	Val  []byte
	Type LRType
}

// LRLoc location of the log record on the disk
type LRLoc struct {
	Fid    uint32
	Offset int64
}

func Encode(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}
