package redis

import (
	"encoding/binary"
	"github.com/berylyvos/yojoudb"
	"math"
	"time"
)

const (
	maxMetadataSize = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraMetaSize   = binary.MaxVarintLen64 * 2
	initListIndex   = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32
	head     uint64
	tail     uint64
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraMetaSize
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	idx := 1
	idx += binary.PutVarint(buf[idx:], md.expire)
	idx += binary.PutVarint(buf[idx:], md.version)
	idx += binary.PutVarint(buf[idx:], int64(md.size))

	if md.dataType == List {
		idx += binary.PutUvarint(buf[idx:], md.head)
		idx += binary.PutUvarint(buf[idx:], md.tail)
	}

	return buf[:idx]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	idx := 1
	expire, n := binary.Varint(buf[idx:])
	idx += n
	version, n := binary.Varint(buf[idx:])
	idx += n
	size, n := binary.Varint(buf[idx:])
	idx += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[idx:])
		idx += n
		tail, _ = binary.Uvarint(buf[idx:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

func (rc *RedisCmd) getMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	metaBuf, err := rc.db.Get(key)
	if err != nil && err != yojoudb.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == yojoudb.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initListIndex
			meta.tail = initListIndex
		}
	}
	return meta, nil
}
