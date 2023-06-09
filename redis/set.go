package redis

import (
	"encoding/binary"
	"yojoudb"
)

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	kl, ml := len(sk.key), len(sk.member)
	buf := make([]byte, kl+ml+8+4)
	idx := 0
	copy(buf[:kl], sk.key)
	idx += kl

	binary.LittleEndian.PutUint64(buf[idx:], uint64(sk.version))
	idx += 8

	copy(buf[idx:], sk.member)
	idx += ml

	binary.LittleEndian.PutUint32(buf[idx:], uint32(ml))
	return buf[:idx]
}

func setEncKey(key, member []byte, ver int64) []byte {
	return (&setInternalKey{
		key:     key,
		version: ver,
		member:  member,
	}).encode()
}

func (rc *RedisCmd) SAdd(key, member []byte) (bool, error) {
	md, err := rc.getMetadata(key, Set)
	if err != nil {
		return false, err
	}

	encKey := setEncKey(key, member, md.version)

	var ok = false
	if _, err = rc.db.Get(encKey); err == yojoudb.ErrKeyNotFound {
		wb := rc.db.NewWriteBatch(yojoudb.DefaultWriteBatchOptions)
		md.size++
		_ = wb.Put(key, md.encode())
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

func (rc *RedisCmd) SIsMember(key, member []byte) (bool, error) {
	md, err := rc.getMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if md.size == 0 {
		return false, nil
	}

	_, err = rc.db.Get(setEncKey(key, member, md.version))
	if err != nil && err != yojoudb.ErrKeyNotFound {
		return false, err
	}
	if err == yojoudb.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

func (rc *RedisCmd) SRem(key, member []byte) (bool, error) {
	md, err := rc.getMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if md.size == 0 {
		return false, nil
	}

	encKey := setEncKey(key, member, md.version)

	if _, err = rc.db.Get(encKey); err == yojoudb.ErrKeyNotFound {
		return false, nil
	}

	wb := rc.db.NewWriteBatch(yojoudb.DefaultWriteBatchOptions)
	md.size--
	_ = wb.Put(key, md.encode())
	_ = wb.Delete(encKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}
