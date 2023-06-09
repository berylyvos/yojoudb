package redis

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
	"yojoudb"
	"yojoudb/utils"
)

func TestRedis_Del_Type(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-del-type")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	defer destroyDB(rds.db)
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

func TestRedis_Set_Get(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-set-get")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	defer destroyDB(rds.db)
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

func TestRedis_HSet_HGet(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-hset-hget")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.TestKey(1), []byte("field1"), utils.RandValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandValue(100)
	ok2, err := rds.HSet(utils.TestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandValue(100)
	ok3, err := rds.HSet(utils.TestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := rds.HGet(utils.TestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := rds.HGet(utils.TestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = rds.HGet(utils.TestKey(1), []byte("field-not-exist"))
	assert.Equal(t, yojoudb.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-hdel")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.TestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := rds.HSet(utils.TestKey(1), []byte("field1"), utils.RandValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandValue(100)
	ok2, err := rds.HSet(utils.TestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.False(t, ok2)

	v2 := utils.RandValue(100)
	ok3, err := rds.HSet(utils.TestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(utils.TestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)
}

func TestRedis_SIsMember(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-sismember")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.TestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.TestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.TestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.TestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(utils.TestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.TestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.TestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := yojoudb.DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-redis-srem")
	opts.DirPath = dir
	rds, err := NewRedisCmd(opts)
	defer destroyDB(rds.db)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.TestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.TestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.TestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.TestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(utils.TestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.TestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func destroyDB(db *yojoudb.DB) {
	if db != nil {
		_ = db.Close()
		err := os.RemoveAll(db.GetDirPath())
		if err != nil {
			panic(err)
		}
	}
}
