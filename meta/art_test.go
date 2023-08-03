package meta

import (
	"bytes"
	"github.com/berylyvos/yojoudb/utils"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sort"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	type args struct {
		key K
		loc Loc
	}
	tests := []struct {
		name   string
		tree   *ART
		args   args
		expect Loc
	}{
		{
			"empty-key", art, args{key: nil, loc: nil}, nil,
		},
		{
			"empty-value", art, args{key: utils.TestKey(1), loc: nil}, nil,
		},
		{
			"valid-key-value", art, args{key: utils.TestKey(1), loc: &Loc1{SegId: 1, BlockIndex: 1, ChunkOffset: 100}}, nil,
		},
		{
			// run this test individually will fail.
			"duplicated-key", art, args{key: utils.TestKey(1), loc: &Loc1{SegId: 2, BlockIndex: 2, ChunkOffset: 200}},
			&Loc1{SegId: 1, BlockIndex: 1, ChunkOffset: 100},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tree.Put(tt.args.key, tt.args.loc); !reflect.DeepEqual(got, tt.expect) {
				t.Errorf("Put() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put(utils.TestKey(1), &Loc1{BlockIndex: 0, ChunkOffset: 123})
	art.Put(utils.TestKey(1), &Loc1{BlockIndex: 42, ChunkOffset: 111})
	type args struct {
		key K
	}
	tests := []struct {
		name   string
		tree   *ART
		args   args
		expect Loc
	}{
		{
			"not-exist", art, args{key: utils.TestKey(10000)}, nil,
		},
		{
			"exist-val", art, args{key: utils.TestKey(1)}, &Loc1{BlockIndex: 42, ChunkOffset: 111},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tree.Get(tt.args.key); !reflect.DeepEqual(got, tt.expect) {
				t.Errorf("Get() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	tree := NewART()
	tree.Put(utils.TestKey(1), &Loc1{BlockIndex: 1, ChunkOffset: 100})
	tree.Put(utils.TestKey(1), &Loc1{BlockIndex: 3, ChunkOffset: 300})
	type args struct {
		key []byte
	}
	tests := []struct {
		name  string
		tree  *ART
		args  args
		want  Loc
		want1 bool
	}{
		{
			"not-exist", tree, args{key: utils.TestKey(6000)}, nil, false,
		},
		{
			"exist", tree, args{key: utils.TestKey(1)}, &Loc1{BlockIndex: 3, ChunkOffset: 300}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := tt.tree.Delete(tt.args.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Delete() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Delete() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	tree := NewART()
	opt := IteratorOpt{Prefix: nil, Reverse: false}

	// empty tree
	iter1 := tree.Iterator(opt)
	defer iter1.Close()
	assert.False(t, iter1.Valid())

	// tree with one node
	tree.Put(utils.TestKey(1), &Loc1{BlockIndex: 1, ChunkOffset: 100})
	iter2 := tree.Iterator(opt)
	defer iter2.Close()
	assert.True(t, iter2.Valid())
	iter2.Next()
	assert.False(t, iter2.Valid())

	testAdaptiveRadixTreeIterator(t, opt, 1000)

	// reverse
	opt.Reverse = true
	testAdaptiveRadixTreeIterator(t, opt, 1000)
}

func TestAdaptiveRadixTreeIterator_Seek(t *testing.T) {
	tree := NewART()
	keys := [][]byte{
		[]byte("ccade"),
		[]byte("aaame"),
		[]byte("aujea"),
		[]byte("ccnea"),
		[]byte("bbeda"),
		[]byte("kkimq"),
		[]byte("neusa"),
		[]byte("mjiue"),
		[]byte("kjuea"),
		[]byte("rnhse"),
		[]byte("mjiqe"),
		[]byte("cjiqe"),
	}
	for _, key := range keys {
		tree.Put(key, &Loc1{BlockIndex: 1, ChunkOffset: 100})
	}

	validate := func(reverse bool, prefix, seek, target []byte) {
		options := IteratorOpt{Prefix: prefix, Reverse: reverse}
		iter := tree.Iterator(options)
		defer iter.Close()

		iter.Seek(seek)
		assert.Equal(t, iter.Key(), target)
	}

	//validate(false, nil, nil, []byte("aaame"))
	validate(false, nil, []byte("mjiue"), []byte("mjiue"))
	validate(false, nil, []byte("bbbb"), []byte("bbeda"))
	validate(true, nil, []byte("ccdes"), []byte("ccade"))
	validate(true, nil, []byte("z"), []byte("rnhse"))

	//validate(false, []byte("c"), []byte("ccn"), []byte("ccnea"))
	//validate(false, []byte("cxxx"), []byte("ccn"), nil)
	//
	//validate(true, []byte("m"), []byte("zzz"), []byte("mjiue"))
	//validate(true, []byte("c"), []byte("ccd"), []byte("ccade"))
}

func TestAdaptiveRadixTreeIterator_Rewind(t *testing.T) {
	tree := NewART()
	keys := [][]byte{
		[]byte("ccade"),
		[]byte("aaame"),
		[]byte("aujea"),
		[]byte("ccnea"),
		[]byte("bbeda"),
		[]byte("kkimq"),
		[]byte("neusa"),
		[]byte("mjiue"),
		[]byte("kjuea"),
		[]byte("rnhse"),
		[]byte("mjiqe"),
		[]byte("cjiqe"),
	}
	for _, key := range keys {
		tree.Put(key, &Loc1{BlockIndex: 1, ChunkOffset: 100})
	}

	validate := func(reverse bool, prefix, seek, target []byte) {
		options := IteratorOpt{Prefix: prefix, Reverse: reverse}
		iter := tree.Iterator(options)
		defer iter.Close()

		if seek != nil {
			iter.Seek(seek)
		}

		iter.Next()
		iter.Next()
		iter.Rewind()
		assert.Equal(t, iter.Key(), target)
	}

	validate(false, nil, []byte("bb"), []byte("aaame"))
	// validate(false, []byte("c"), []byte("bb"), []byte("ccade"))
}

func testAdaptiveRadixTreeIterator(t *testing.T, options IteratorOpt, size int) {
	tree := NewART()
	var keys [][]byte
	for i := 0; i < size; i++ {
		key := utils.RandValue(10)
		keys = append(keys, key)
		tree.Put(key, &Loc1{BlockIndex: 1, ChunkOffset: 100})
	}

	sort.Slice(keys, func(i, j int) bool {
		if options.Reverse {
			return bytes.Compare(keys[i], keys[j]) > 0
		} else {
			return bytes.Compare(keys[i], keys[j]) < 0
		}
	})

	var i = 0
	iter := tree.Iterator(options)
	defer iter.Close()
	for ; iter.Valid(); iter.Next() {
		assert.Equal(t, keys[i], iter.Key())
		i++
	}
	assert.Equal(t, i, size)
}
