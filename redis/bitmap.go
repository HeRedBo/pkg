package redis

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
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

func (r *Client) SetBit(ctx context.Context, key string, offset int64, val int) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	realKey := GetKey(key, offset)
	value, err = r.client.SetBit(ctx, realKey, GetOffset(offset), val).Result()
	//TODO 记录错误日志
	return
}

func (r *Client) GetBit(ctx context.Context, key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题
	realKey := GetKey(key, offset)
	value, err = r.client.GetBit(ctx, realKey, GetOffset(offset)).Result()
	if err != nil {
		return value, err
	}
	return
}

func (r *Client) SetBitBit(ctx context.Context, key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	realKey := GetBigKey(key, offset)
	value, err = r.client.GetBit(ctx, realKey, GetBigOffset(offset)).Result()
	if err != nil {
		return value, err
	}
	return
}

func (r *Client) GetBigBit(ctx context.Context, key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key")
		return
	}
	realKey := GetBigKey(key, offset)

	//集群版为了避免单个bitmap只会落到集群中的一个节点，这里默认对bitmap进行分捅，以平衡redis集群负载，防止单个bitmap热点问题
	//对于超过redis bitmap范围的数据，采用不同的分捅方式
	value, err = r.client.GetBit(ctx, realKey, GetBigOffset(offset)).Result()
	if err != nil {
		return value, err
	}
	return
}

func (r *Client) SetBitNOBucket(ctx context.Context, key string, offset int64, val int) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key ")
		return
	}

	value, err = r.client.SetBit(ctx, key, offset, val).Result()
	if err != nil {
		return value, err
	}
	return
}

func (r *Client) GetBitNOBucket(ctx context.Context, key string, offset int64) (value int64, err error) {
	if len(key) == 0 {
		err = errors.New("empty key ")
		return
	}
	value, err = r.client.GetBit(ctx, key, offset).Result()
	if err != nil {
		return value, err
	}
	return
}

func (r *Client) BitCountNOBucket(ctx context.Context, op, destKey string, keys ...string) (value int64, err error) {
	if len(keys) == 0 {
		err = errors.New("empty key ")
		return
	}
	var cmd *redis.IntCmd
	op = strings.ToUpper(op)
	if r.client != nil {
		switch op {
		case "AND":
			cmd = r.client.BitOpAnd(ctx, destKey, keys...)
		case "OR":
			cmd = r.client.BitOpOr(ctx, destKey, keys...)
		case "XOR":
			cmd = r.client.BitOpXor(ctx, destKey, keys...)
		case "NOT":
			cmd = r.client.BitOpNot(ctx, destKey, keys[0])
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
		cmd = r.client.BitOpAnd(ctx, destKey, keys...)
	case "OR":
		cmd = r.client.BitOpOr(ctx, destKey, keys...)
	case "XOR":
		cmd = r.client.BitOpXor(ctx, destKey, keys...)
	case "NOT":
		cmd = r.client.BitOpNot(ctx, destKey, keys[0])
	default:
		return 0, errors.New("illegal op " + op + "; key:" + destKey)
	}
	value, err = cmd.Result()
	if err != nil {
		return value, err
	}
	return
}
