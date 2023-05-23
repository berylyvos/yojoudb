package yojoudb

import "errors"

var (
	ErrKeyEmpty          = errors.New("KeyEmptyError : the key is empty")
	ErrIndexUpdateFailed = errors.New("IndexUpdateFailError : failed to update index")
	ErrKeyNotFound       = errors.New("KeyNotFoundError : key is not found in database")
	ErrDataFileNotFound  = errors.New("DataFileNotFoundError : data file is not found")
	ErrDataDirBroken     = errors.New("DataDirBrokenError : the databases directory maybe broken")
)

var (
	ErrDirPathIsEmpty          = errors.New("DirPathError : database dir path is empty")
	ErrDataFileSizeNotPositive = errors.New("DataFileSizeNotPositiveError : database data file size must be greater than 0")
)
