package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"yojoudb/utils"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// dont commit after writes
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.TestKey(1), utils.RandValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.TestKey(2))
	assert.Nil(t, err)
	_, err = db.Get(utils.TestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// commit
	err = wb.Commit()
	assert.Nil(t, err)
	val1, err := db.Get(utils.TestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// delete and commit
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.TestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)
	_, err = db.Get(utils.TestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-batch-2")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.TestKey(1), utils.RandValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.TestKey(2), utils.RandValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.TestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.TestKey(11), utils.RandValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// restart
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.TestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)

	// check seqNo
	assert.Equal(t, uint64(2), db.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "yojoudb-test-batch-3")
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//keys := db.ListKeys()
	//t.Log(len(keys))

	wbOpts := DefaultWriteBatchOptions
	wbOpts.MaxBatchNum = 10000000
	wb := db.NewWriteBatch(wbOpts)
	for i := 0; i < 500000; i++ {
		err := wb.Put(utils.TestKey(i), utils.RandValue(1024))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)
}
