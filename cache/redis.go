package cache

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"time"
)

const (
	PrefixChainLastDigest  = "chain::%s-package::%s-last_digest"
	prefixQueueMessageLock = "failed_queue_message:%d"
)

type Redis struct {
	*redis.Client
	script map[string]string
}

func NewRedis(rc *redis.Client) (*Redis, error) {
	result := &Redis{
		Client: rc,
	}
	err := result.InitScript()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *Redis) GetChainPackageLastDigest(chain, packageId string) string {
	key := fmt.Sprintf(PrefixChainLastDigest, chain, packageId)
	result, err := r.Get(r.Context(), key).Result()
	if err != nil {
		if err != redis.Nil {
			log.Errorf("get key: %s value with failed: %v", key, err)
		}
		return ""
	}
	return result
}

func (r *Redis) SetQueueConsumeLock(queueId int64) bool {
	key := fmt.Sprintf(prefixQueueMessageLock, queueId)
	result, err := r.SetNX(r.Context(), key, 1, 5*time.Minute).Result()
	if err != nil {
		log.Errorf("setnx for key %s failed with err: %v", key, err)
		return false
	}
	return result
}

func (r *Redis) UnlockQueueConsumeLock(queueId int64) {
	r.Del(r.Context(), fmt.Sprintf(prefixQueueMessageLock, queueId))
}

func (r *Redis) ClearKey(keys ...string) error {
	_, err := r.Del(r.Context(), keys...).Result()
	if err != nil {
		return err
	}
	return nil
}
