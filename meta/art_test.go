package meta

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yojoudb/data"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	res1 := art.Put([]byte("key-1"), &data.LRLoc{Fid: 1, Offset: 11})
	assert.Nil(t, res1)
	res2 := art.Put([]byte("key-2"), &data.LRLoc{Fid: 1, Offset: 12})
	assert.Nil(t, res2)
	res3 := art.Put([]byte("key-3"), &data.LRLoc{Fid: 1, Offset: 13})
	assert.Nil(t, res3)

	res4 := art.Put([]byte("key-3"), &data.LRLoc{Fid: 2, Offset: 22})
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(13), res4.Offset)
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LRLoc{Fid: 1, Offset: 11})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-1"), &data.LRLoc{Fid: 2, Offset: 22})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-1"), &data.LRLoc{Fid: 1, Offset: 42})
	res2, ok2 := art.Delete([]byte("key-1"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.Fid)
	assert.Equal(t, int64(42), res2.Offset)

	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LRLoc{Fid: 1, Offset: 11})
	art.Put([]byte("key-2"), &data.LRLoc{Fid: 1, Offset: 22})
	art.Put([]byte("key-1"), &data.LRLoc{Fid: 1, Offset: 12})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("Akira"), &data.LRLoc{Fid: 1, Offset: 11})
	art.Put([]byte("Bryce"), &data.LRLoc{Fid: 1, Offset: 11})
	art.Put([]byte("Babylon"), &data.LRLoc{Fid: 1, Offset: 11})
	art.Put([]byte("Karma"), &data.LRLoc{Fid: 1, Offset: 11})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()), iter.Value())
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
