package yojoudb

import "errors"

var (
	ErrKeyEmpty                = errors.New("the key is empty")
	ErrIndexUpdateFailed       = errors.New("failed to update index")
	ErrKeyNotFound             = errors.New("key is not found in database")
	ErrDataFileNotFound        = errors.New("data file is not found")
	ErrDataDirBroken           = errors.New("the databases directory maybe broken")
	ErrExceedMaxBatchNum       = errors.New("exceed the max batch num")
	ErrMergeIsProgress         = errors.New("merge is in progress, try again later")
	ErrDirPathIsEmpty          = errors.New("database dir path is empty")
	ErrDataFileSizeNotPositive = errors.New("database data file size must be greater than 0")
	ErrDatabaseIsUsing         = errors.New("the database directory is used by another process")
	ErrInvalidMergeRatio       = errors.New("invalid merge ratio, must between 0 and 1")
	ErrMergeRatioUnreached     = errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge   = errors.New("no enough disk space for merge")
	ErrDBClosed                = errors.New("the database is closed")
	ErrReadOnlyBatch           = errors.New("the batch is read only")
)
