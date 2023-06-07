package redis

import (
	"encoding/binary"
	"errors"
	"time"
	"yojoudb"
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

func NewRedisCmd(options *yojoudb.Options) (*RedisCmd, error) {
	db, err := yojoudb.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisCmd{db: db}, nil
}

//*=============== String ===============*//

func (rc *RedisCmd) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// type | expire | payload
	b := make([]byte, 1+binary.MaxVarintLen64)
	b[0] = String
	idx := 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	idx += binary.PutVarint(b[idx:], expire)

	encVal := make([]byte, idx+len(value))
	copy(encVal[:idx], b[:idx])
	copy(encVal[idx:], value)

	return rc.db.Put(key, encVal)
}

func (rc *RedisCmd) Get(key []byte) ([]byte, error) {
	encVal, err := rc.db.Get(key)
	if err != nil {
		return nil, err
	}

	if encVal[0] != String {
		return nil, ErrWrongTypeOperation
	}

	idx := 1
	expire, n := binary.Varint(encVal[idx:])
	idx += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encVal[idx:], nil
}
