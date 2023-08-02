package yojoudb

import "errors"

var (
	ErrKeyEmpty    = errors.New("the key is empty")
	ErrKeyNotFound = errors.New("key is not found in database")

	ErrDirPathIsEmpty          = errors.New("database dir path is empty")
	ErrDataFileSizeNotPositive = errors.New("database data file size must be greater than 0")
	ErrDatabaseIsUsing         = errors.New("the database directory is used by another process")

	ErrDBClosed       = errors.New("the database is closed")
	ErrReadOnlyBatch  = errors.New("the batch is read only")
	ErrMergeIsRunning = errors.New("merge is in progress, try again later")
)
