package redis

import (
	"encoding/binary"
	"github.com/berylyvos/yojoudb"
	"github.com/berylyvos/yojoudb/utils"
)

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zsetInternalKey) encodeMember() []byte {
	kl, ml := len(zk.key), len(zk.member)
	b := make([]byte, kl+8+ml)

	idx := 0
	copy(b[:kl], zk.key)
	idx += kl

	binary.LittleEndian.PutUint64(b[idx:], uint64(zk.version))
	idx += 8

	copy(b[idx:], zk.member)
	return b
}

func (zk *zsetInternalKey) encodeScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	kl, ml, sl := len(zk.key), len(zk.member), len(scoreBuf)
	b := make([]byte, kl+ml+sl+8+4)

	idx := 0
	copy(b[:kl], zk.key)
	idx += kl

	binary.LittleEndian.PutUint64(b[idx:], uint64(zk.version))
	idx += 8

	copy(b[idx:], scoreBuf)
	idx += sl

	copy(b[idx:], zk.member)
	idx += ml

	binary.LittleEndian.PutUint32(b[idx:], uint32(ml))
	return b
}

func (rc *RedisCmd) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	md, err := rc.getMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:     key,
		version: md.version,
		member:  member,
		score:   score,
	}
	memberKey := zk.encodeMember()

	var exist = false
	val, _ := rc.db.Get(memberKey)
	if val != nil {
		if score == utils.FloatFromBytes(val) {
			return false, nil
		}
		exist = true
	}

	wb := rc.db.NewBatch(yojoudb.DefaultBatchOptions)
	if !exist {
		md.size++
		_ = wb.Put(key, md.encode())
	} else {
		oldKey := &zsetInternalKey{
			key:     key,
			version: md.version,
			member:  member,
			score:   utils.FloatFromBytes(val),
		}
		_ = wb.Delete(oldKey.encodeScore())
	}

	_ = wb.Put(memberKey, utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rc *RedisCmd) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rc.getMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	val, err := rc.db.Get(zk.encodeMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(val), nil
}
