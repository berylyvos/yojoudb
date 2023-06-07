package redis

import "errors"

func (rc *RedisCmd) Del(key []byte) error {
	return rc.db.Delete(key)
}

func (rc *RedisCmd) Type(key []byte) (RedisDataType, error) {
	encValue, err := rc.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}
	return encValue[0], nil
}
