package meta

import (
	"go.etcd.io/bbolt"
	"path/filepath"
	"yojoudb/data"
)

const bptIndexFileName = "bpt-index"

var indexBucketName = []byte("bucket-index")

// BPlusTree on-disk index based on bbolt.DB
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites
	db, err := bbolt.Open(filepath.Join(dirPath, bptIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bbolt")
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bbolt")
	}

	return &BPlusTree{tree: db}
}

func (bpt *BPlusTree) Put(key K, loc Loc) Loc {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldVal = bucket.Get(key)
		return bucket.Put(key, data.EncodeLRLoc(loc))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldVal) == 0 {
		return nil
	}
	return data.DecodeLRLoc(oldVal)
}

func (bpt *BPlusTree) Get(key K) Loc {
	var loc Loc
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		val := tx.Bucket(indexBucketName).Get(key)
		if len(val) != 0 {
			loc = data.DecodeLRLoc(val)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return loc
}

func (bpt *BPlusTree) Delete(key K) (Loc, bool) {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldVal = bucket.Get(key); len(oldVal) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(oldVal) == 0 {
		return nil, false
	}
	return data.DecodeLRLoc(oldVal), true
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPTreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Size() int {
	var sz int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		sz = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return sz
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBPTreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Last()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.First()
	}
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.currKey, bpi.currValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.currKey, bpi.currValue = bpi.cursor.Prev()
	} else {
		bpi.currKey, bpi.currValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.currKey) != 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.currKey
}

func (bpi *bptreeIterator) Value() Loc {
	return data.DecodeLRLoc(bpi.currValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
