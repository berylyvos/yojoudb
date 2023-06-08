package redis

import (
	"encoding/binary"
	"yojoudb"
)

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8)
	idx := 0
	copy(buf[:len(hk.key)], hk.key)
	idx += len(hk.key)

	binary.LittleEndian.PutUint64(buf[idx:idx+8], uint64(hk.version))
	idx += 8

	copy(buf[idx:], hk.field)

	return buf
}

func getEncKey(key, field []byte, ver int64) []byte {
	return (&hashInternalKey{
		key:     key,
		version: ver,
		field:   field,
	}).encode()
}

func (rc *RedisCmd) HSet(key, field, value []byte) (bool, error) {
	md, err := rc.getMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	encKey := getEncKey(key, field, md.version)

	var exist = true
	if v, _ := rc.db.Get(encKey); v == nil {
		exist = false
	}

	wb := rc.db.NewWriteBatch(yojoudb.DefaultWriteBatchOptions)
	if !exist {
		md.size++
		_ = wb.Put(key, md.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rc *RedisCmd) HGet(key, field []byte) ([]byte, error) {
	md, err := rc.getMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if md.size == 0 {
		return nil, nil
	}

	encKey := getEncKey(key, field, md.version)

	return rc.db.Get(encKey)
}

func (rc *RedisCmd) HDel(key, field []byte) (bool, error) {
	md, err := rc.getMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if md.size == 0 {
		return false, nil
	}

	encKey := getEncKey(key, field, md.version)

	var exist = true
	if v, _ := rc.db.Get(encKey); v == nil {
		exist = false
	}

	if exist {
		wb := rc.db.NewWriteBatch(yojoudb.DefaultWriteBatchOptions)
		md.size--
		_ = wb.Put(encKey, md.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}
