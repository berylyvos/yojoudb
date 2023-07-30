package meta

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb"
)

func TestSkiplist_Put(t *testing.T) {
	sl := NewSkiplist()
	res1 := sl.Put([]byte("key-1"), &yojoudb.LRLoc{Fid: 1, Offset: 11})
	assert.Nil(t, res1)
	res2 := sl.Put([]byte("key-2"), &yojoudb.LRLoc{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := sl.Put([]byte("key-3"), &yojoudb.LRLoc{Fid: 1, Offset: 13})
	assert.Nil(t, res3)

	res4 := sl.Put([]byte("key-3"), &yojoudb.LRLoc{Fid: 2, Offset: 22})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(13), res4.Offset)
}

func TestSkiplist_Get(t *testing.T) {
	sl := NewSkiplist()
	sl.Put([]byte("key-1"), &yojoudb.LRLoc{Fid: 1, Offset: 11})
	pos := sl.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := sl.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	sl.Put([]byte("key-1"), &yojoudb.LRLoc{Fid: 2, Offset: 22})
	pos2 := sl.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

func TestSkiplist_Delete(t *testing.T) {
	sl := NewSkiplist()
	res1, ok1 := sl.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	sl.Put([]byte("key-1"), &yojoudb.LRLoc{Fid: 1, Offset: 42})
	res2, ok2 := sl.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(42), res2.Offset)

	pos := sl.Get([]byte("key-1"))
	assert.Nil(t, pos)
}
