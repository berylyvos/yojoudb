package redis

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
	"yojoudb"
	"yojoudb/utils"
)

func TestRedis_Set_Get(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-set-get")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.TestKey(1), 0, utils.RandValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.TestKey(2), time.Second*3, utils.RandValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.TestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.TestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.TestKey(42))
	assert.Equal(t, yojoudb.ErrKeyNotFound, err)
}
