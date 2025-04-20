package cache

import (
	"fmt"
	"github.com/HeRedBo/pkg/errors"
	"github.com/go-redis/redis/v7"
	"strings"
)

// 对于超出redis bitmap范围的数据我们使用高49位作捅，低15为作offset

// GetBigBucket 高49位作捅，低15为作offset
func GetBigBucket(ID int64) int64 {
	return ID >> 15
}

// GetBigOffset  0x7FFF的二进制为111111111111111
// 与ID做与运算结果保留了ID的低15位
func GetBigOffset(ID int64) int64 {
	return ID & 0x7FFF
}

func GetBucket(userID int64) int64 {
	return userID & 0x7FFF
}

func GetOffset(userID int64) int64 {
	return userID >> 16
}

func GetKey(key string, ID int64) string {
	return fmt.Sprintf("%s_%d", key, GetBucket(ID))
}

func GetBigKey(key string, ID int64) string {
	return fmt.Sprintf("%s_%d", key, GetBigBucket(ID))
}

func (r *Redis) SetBit(key string, offset int64, val int) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}

	realKey := GetKey(key, offset)
	if r.client != nil {
		value, err := r.client.SetBit(realKey, GetOffset(offset), val).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
			//return value, err
		}
	}
	r.clusterClient.SetBit(realKey, GetOffset(offset), val).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
		//return value, err
	}
	return
}

func (r *Redis) GetBit(key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题
	realKey := GetKey(key, offset)

	if r.client != nil {
		value, err = r.client.GetBit(realKey, GetOffset(offset)).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
			//return value, err
		}
	}

	value, err = r.clusterClient.GetBit(realKey, GetOffset(offset)).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
		//return value, err
	}
	return
}

func (r *Redis) SetBigBit(key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	realKey := GetBigKey(key, offset)
	if r.client != nil {
		value, err = r.client.GetBit(realKey, GetBigOffset(offset)).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
		}
	}

	value, err = r.clusterClient.GetBit(realKey, GetBigOffset(offset)).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis setbit key: %s err", realKey)
		//return value, err
	}
	return
}

func (r *Redis) GetBigBit(key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	realKey := GetBigKey(key, offset)
	if r.client != nil {
		value, err = r.client.GetBit(realKey, GetOffset(offset)).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
		}
	}
	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题
	//对于超过redis bitmap范围的数据，采用不同的分捅方式
	value, err = r.clusterClient.GetBit(realKey, GetBigOffset(offset)).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis getbit key: %s err", realKey)
	}
	return
}

func (r *Redis) SetBitNOBucket(key string, offset int64, val int) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key ")
		return
	}

	if r.client != nil {
		value, err = r.client.SetBit(key, offset, val).Result()
		if err != nil {
			return value, err
		}
		return
	}
	value, err = r.clusterClient.SetBit(key, offset, val).Result()
	if err != nil {
		return value, err
	}
	return
}

func (r *Redis) GetBitNOBucket(key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key ")
		return
	}
	if r.client != nil {
		value, err = r.client.GetBit(key, offset).Result()
		if err != nil {
			return value, errors.Wrapf(err, "redis getbit key: %s err", key)
			//return value, err
		}
		return
	}
	value, err = r.clusterClient.GetBit(key, offset).Result()
	if err != nil {
		return value, errors.Wrapf(err, "redis getbit key: %s err", key)
	}
	return
}

func (r *Redis) BitCountNOBucket(op, destKey string, keys ...string) (value int64, err error) {
	if len(keys) == 0 {
		err = errors.New("empty key ")
		return
	}
	var cmd *redis.IntCmd
	op = strings.ToUpper(op)
	if r.client != nil {
		switch op {
		case "AND":
			cmd = r.client.BitOpAnd(destKey, keys...)
		case "OR":
			cmd = r.client.BitOpOr(destKey, keys...)
		case "XOR":
			cmd = r.client.BitOpXor(destKey, keys...)
		case "NOT":
			cmd = r.client.BitOpNot(destKey, keys[0])
		default:
			return 0, errors.New("illegal op " + op + "; key:" + destKey)
		}
		value, err = cmd.Result()
		if err != nil {
			return value, err
		}
		return
	}

	switch op {
	case "AND":
		cmd = r.client.BitOpAnd(destKey, keys...)
	case "OR":
		cmd = r.client.BitOpOr(destKey, keys...)
	case "XOR":
		cmd = r.client.BitOpXor(destKey, keys...)
	case "NOT":
		cmd = r.client.BitOpNot(destKey, keys[0])
	default:
		return 0, errors.New("illegal op " + op + "; key:" + destKey)
	}
	value, err = cmd.Result()
	if err != nil {
		return value, err
	}
	return

}
