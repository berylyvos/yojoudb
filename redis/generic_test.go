package redis

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"yojoudb"
	"yojoudb/utils"
)

func TestRedis_Del_Type(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	assert.Nil(t, err)

	// del
	err = rds.Del(utils.TestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.TestKey(1), 0, utils.RandValue(100))
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(utils.TestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.TestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.TestKey(1))
	assert.Equal(t, yojoudb.ErrKeyNotFound, err)
}
