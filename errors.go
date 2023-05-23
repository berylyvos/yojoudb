package yojoudb

import "errors"

var (
	ErrKeyEmpty          = errors.New("KeyEmptyError : the key is empty")
	ErrIndexUpdateFailed = errors.New("IndexUpdateFailError : failed to update index")
	ErrKeyNotFound       = errors.New("KeyNotFoundError : key is not found in database")
	ErrDataFileNotFound  = errors.New("DataFileNotFoundError : data file is not found")
)
