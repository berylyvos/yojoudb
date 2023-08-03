package redis

import (
	"errors"
	"github.com/berylyvos/yojoudb"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

type RedisCmd struct {
	db *yojoudb.DB
}

func NewRedisCmd(options yojoudb.Options) (*RedisCmd, error) {
	db, err := yojoudb.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisCmd{db: db}, nil
}
