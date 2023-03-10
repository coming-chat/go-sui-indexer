package cache

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"strconv"
	"time"
)

func (r *Redis) ReadGroupMessages(topic, group, customer string, count int64) ([]redis.XMessage, error) {
	arg := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: customer,
		Streams:  []string{topic, ">"},
		Count:    count,
		Block:    5 * time.Second,
		NoAck:    false,
	}
	for {
		result, err := r.XReadGroup(r.Context(), arg).Result()
		if err != nil {
			if err == redis.Nil {
				continue
			}
			return nil, err
		}
		if len(result) != 1 {
			continue
		}
		return result[0].Messages, nil
	}
}

func (r *Redis) RelyAck(topic, group string, message redis.XMessage) error {
	result, err := r.XAck(r.Context(), topic, group, message.ID).Result()
	if err != nil {
		return err
	}
	if result == 1 {
		return nil
	} else {
		return fmt.Errorf("rely ack for message id %s get unknown: %d", message.ID, result)
	}
}

func (r *Redis) GetCustomerPendingMessage(arg *redis.XPendingExtArgs) ([]redis.XPendingExt, error) {
	result, err := r.XPendingExt(r.Context(), arg).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	return result, nil
}

func (r *Redis) ClaimPendingMessage(arg *redis.XClaimArgs) ([]redis.XMessage, error) {
	return r.XClaim(r.Context(), arg).Result()
}

func (r *Redis) PushMessageToStream(arg *redis.XAddArgs) error {
	_, err := r.XAdd(r.Context(), arg).Result()
	if err != nil {
		return err
	}
	return nil
}

func (r *Redis) CreateGroup(topic, groupId, start string) error {
	_, err := r.XGroupCreateMkStream(r.Context(), topic, groupId, start).Result()
	return err
}

func (r *Redis) GetTopicGroups(topic string) ([]redis.XInfoGroup, error) {
	return r.XInfoGroups(r.Context(), topic).Result()
}

func (r *Redis) GetConsumer(topic, group string) ([]redis.XInfoConsumer, error) {
	return r.XInfoConsumers(r.Context(), topic, group).Result()
}

func (r *Redis) CreateConsumer(topic, group, consumer string) error {
	_, err := r.XGroupCreateConsumer(r.Context(), topic, group, consumer).Result()
	return err
}

func (r *Redis) GetPendingInfo(topic, group string) (*redis.XPending, error) {
	return r.XPending(r.Context(), topic, group).Result()
}

func (r *Redis) TrimStream(topic, miniID string) error {
	_, err := r.XTrimMinIDApprox(r.Context(), topic, miniID, 0).Result()
	return err
}

func (r *Redis) StreamLen(topic string) (int64, error) {
	return r.XLen(r.Context(), topic).Result()
}

func (r *Redis) GetLastTransactionNumber(topic string) int64 {
	result, err := r.XInfoStream(r.Context(), topic).Result()
	if err != nil {
		log.Errorf("get xinfo stream %s with err: %v", topic, err)
		return 0
	}
	if result.LastEntry.ID == "" {
		return 0
	}
	num, err := strconv.ParseInt(result.LastEntry.Values["data"].(string), 10, 64)
	if err != nil {
		log.Errorf("get transaction last job number err: %v", err)
		return 0
	}
	return num
}

func (r *Redis) PushNewTxDigestToStream(topic, chain, packageId, preDigest, digest string) {
	rpip := r.Pipeline()
	defer rpip.Close()
	rpip.EvalSha(
		r.Context(),
		r.script["pushNewTxDigestToStream"],
		[]string{topic, fmt.Sprintf(PrefixChainLastDigest, chain, packageId)},
		"data",
		preDigest,
		digest,
	)
	_, err := rpip.Exec(r.Context())
	if err != nil {
		log.Errorf("push tx to stream %s err: %v", topic, err)
	}
}
