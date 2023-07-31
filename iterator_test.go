package yojoudb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb/utils"
)

func TestIterator_Normal(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)

	// empty database
	iter1 := db.NewIterator(DefaultIteratorOptions)
	assert.False(t, iter1.Valid())
	iter2 := db.NewIterator(IteratorOptions{Reverse: true, Prefix: []byte("aa")})
	assert.False(t, iter2.Valid())

	// with data
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.TestKey(i), utils.RandValue(4*KB))
		assert.Nil(t, err)
	}
	iter3 := db.NewIterator(DefaultIteratorOptions)
	defer iter3.Close()
	var i = 0
	for ; iter3.Valid(); iter3.Next() {
		value, err := iter3.Value()
		assert.Nil(t, err)
		assert.NotNil(t, value)
		i++
	}
	assert.Equal(t, 10000, i)
}
