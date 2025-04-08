package redis

import (
	"context"
	"errors"
	"fmt"
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
