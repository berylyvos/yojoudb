package yojoudb

import "errors"

var (
	ErrKeyEmpty          = errors.New("KeyEmptyError : the key is empty")
	ErrIndexUpdateFailed = errors.New("IndexUpdateFailError : failed to update index")
)
