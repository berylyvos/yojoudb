package meta

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &yojoudb.LRLoc{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &yojoudb.LRLoc{Fid: 1, Offset: 200})
	assert.Nil(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &yojoudb.LRLoc{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	pst1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pst1.Fid)
	assert.Equal(t, int64(100), pst1.Offset)

	res2 := bt.Put([]byte("a"), &yojoudb.LRLoc{Fid: 1, Offset: 200})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &yojoudb.LRLoc{Fid: 1, Offset: 300})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(200))

	pst2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pst2.Fid)
	assert.Equal(t, int64(300), pst2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &yojoudb.LRLoc{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, ok1 := bt.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2.Fid, uint32(1))
	assert.Equal(t, res2.Offset, int64(100))

	res3 := bt.Put([]byte("aaa"), &yojoudb.LRLoc{Fid: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, res4.Fid, uint32(22))
	assert.Equal(t, res4.Offset, int64(33))
}

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// put one record
	bt1.Put([]byte("aaa"), &yojoudb.LRLoc{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// iterate records
	bt1.Put([]byte("aaa"), &yojoudb.LRLoc{Fid: 1, Offset: 0})
	bt1.Put([]byte("bbb"), &yojoudb.LRLoc{Fid: 1, Offset: 11})
	bt1.Put([]byte("ccc"), &yojoudb.LRLoc{Fid: 1, Offset: 22})
	bt1.Put([]byte("zzz"), &yojoudb.LRLoc{Fid: 1, Offset: 22})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	// reverse
	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("b")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
		t.Log(string(iter5.Key()))
	}

	// reverse seek
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("z")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
		t.Log(string(iter6.Key()))
	}
}
