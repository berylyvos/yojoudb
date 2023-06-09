package redis

import (
	"encoding/binary"
	"yojoudb"
)

type listInternalKey struct {
	key     []byte
	version int64
	idx     uint64
}

func (lk *listInternalKey) encode() []byte {
	kLen := len(lk.key)
	buf := make([]byte, kLen+8+8)
	idx := 0
	copy(buf[:kLen], lk.key)
	idx += kLen

	binary.LittleEndian.PutUint64(buf[idx:], uint64(lk.version))
	idx += 8

	binary.LittleEndian.PutUint64(buf[idx:], lk.idx)
	return buf
}

func listEncKey(key []byte, ver int64, idx uint64) []byte {
	return (&listInternalKey{
		key:     key,
		version: ver,
		idx:     idx,
	}).encode()
}

func (rc *RedisCmd) LPush(key, element []byte) (uint32, error) {
	return rc.push(key, element, true)
}

func (rc *RedisCmd) RPush(key, element []byte) (uint32, error) {
	return rc.push(key, element, false)
}

func (rc *RedisCmd) LPop(key []byte) ([]byte, error) {
	return rc.pop(key, true)
}

func (rc *RedisCmd) RPop(key []byte) ([]byte, error) {
	return rc.pop(key, false)
}

func (rc *RedisCmd) push(key, element []byte, isLeft bool) (uint32, error) {
	md, err := rc.getMetadata(key, List)
	if err != nil {
		return 0, err
	}

	var idx uint64 = 0
	if isLeft {
		idx = md.head - 1
	} else {
		idx = md.tail
	}

	encKey := listEncKey(key, md.version, idx)
	wb := rc.db.NewWriteBatch(yojoudb.DefaultWriteBatchOptions)
	md.size++
	if isLeft {
		md.head--
	} else {
		md.tail++
	}
	_ = wb.Put(key, md.encode())
	_ = wb.Put(encKey, element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}

	return md.size, nil
}

func (rc *RedisCmd) pop(key []byte, isLeft bool) ([]byte, error) {
	md, err := rc.getMetadata(key, List)
	if err != nil {
		return nil, err
	}

	var idx uint64 = 0
	if isLeft {
		idx = md.head
	} else {
		idx = md.tail - 1
	}

	encKey := listEncKey(key, md.version, idx)
	elem, err := rc.db.Get(encKey)
	if err != nil {
		return nil, err
	}

	md.size--
	if isLeft {
		md.head++
	} else {
		md.tail--
	}
	if err = rc.db.Put(key, md.encode()); err != nil {
		return nil, err
	}

	return elem, nil
}
