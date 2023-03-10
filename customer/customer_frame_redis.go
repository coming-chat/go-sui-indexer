package customer

import (
	"context"
	"errors"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"strings"
	"sync"
	"time"
)

const RedisFrameType = "redis"

type (
	RedisCustomer interface {
		SubCustomer
		ConsumeRedisMessage(message redis.XMessage) error
		ProcessRedisFailedMessage(message redis.XMessage, fromErr error) error
	}

	BaseRedisCustomer struct {
		groupId          string
		topic            string
		customer         string
		customerQuantity int
		redisClient      *cache.Redis
		customerPool     chan struct{}
		msgCount         int64
		wg               sync.WaitGroup
		consumeMsg       func(message redis.XMessage) error
		consumeErrMsg    func(message redis.XMessage, fromErr error) error
	}
)

func NewBaseRedisCustomer(c config.QueueConfig, cache *cache.Redis) BaseRedisCustomer {
	return BaseRedisCustomer{
		redisClient:      cache,
		wg:               sync.WaitGroup{},
		topic:            c.RedisStreamConfig.Topic,
		groupId:          c.RedisStreamConfig.GroupID,
		customer:         c.RedisStreamConfig.Customer,
		msgCount:         c.RedisStreamConfig.MsgCount,
		customerQuantity: c.RedisStreamConfig.CustomerQuantity,
		customerPool:     make(chan struct{}, c.RedisStreamConfig.CustomerPool),
	}
}

func (r *BaseRedisCustomer) InitCustomer() error {
	var (
		NeedCreateGroup   = true
		NeedCreateConsume = true
	)
	groups, err := r.redisClient.GetTopicGroups(r.GetTopic())
	if err != nil && strings.Index(err.Error(), "ERR no such key") == -1 {
		return err
	} else if err == nil {
		for _, v := range groups {
			if v.Name != r.GetGroupId() {
				continue
			}
			NeedCreateGroup = false
			var consumers []redis.XInfoConsumer
			consumers, err = r.redisClient.GetConsumer(r.GetTopic(), r.GetGroupId())
			if err != nil && strings.Index(err.Error(), "ERR no such key") == -1 {
				return err
			} else if err == nil {
				for _, v := range consumers {
					if v.Name == r.customer {
						NeedCreateConsume = false
						return nil
					}
				}
			}
		}
	}

	if NeedCreateGroup {
		err = r.redisClient.CreateGroup(r.topic, r.groupId, "0-0")
		if err != nil {
			return err
		}
	}

	if NeedCreateConsume {
		err = r.redisClient.CreateConsumer(r.GetTopic(), r.GetGroupId(), r.customer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *BaseRedisCustomer) GetCustomerType() string {
	return RedisFrameType
}

func (r *BaseRedisCustomer) GetTopic() string {
	return r.topic
}

func (r *BaseRedisCustomer) GetGroupId() string {
	return r.groupId
}

func (r *BaseRedisCustomer) GetCustomerQuantity() int {
	return r.customerQuantity
}

func (r *BaseRedisCustomer) ConsumeMessage(message interface{}) error {
	redisMessage, ok := message.(redis.XMessage)
	if !ok {
		return errors.New("this message not the redis stream message")
	}
	return r.consumeMsg(redisMessage)
}

func (r *BaseRedisCustomer) ConsumePendingMessage() {
	pendingMsgExt, err := r.getPendingMessageAndConsume("-")
	if err != nil {
		log.Errorf("consume pending message failed: %v", err)
	}
	for len(pendingMsgExt) == int(r.msgCount) {
		pendingMsgExt, err = r.getPendingMessageAndConsume(pendingMsgExt[r.msgCount-1].ID)
		if err != nil {
			if err == redis.Nil {
				return
			}
			log.Errorf("cycle consume pending message failed: %v", err)
			continue
		}
	}
}

func (r *BaseRedisCustomer) getPendingMessageAndConsume(start string) ([]redis.XPendingExt, error) {
	pendingMessages, err := r.redisClient.GetCustomerPendingMessage(&redis.XPendingExtArgs{
		Stream:   r.GetTopic(),
		Group:    r.GetGroupId(),
		Start:    start,
		End:      "+",
		Count:    r.msgCount,
		Consumer: r.customer,
	})
	if err != nil {
		return nil, err
	}
	if len(pendingMessages) == 0 {
		return nil, nil
	}
	var messageIds []string
	for _, v := range pendingMessages {
		messageIds = append(messageIds, v.ID)
	}
	messages, err := r.redisClient.ClaimPendingMessage(&redis.XClaimArgs{
		Group:    r.GetGroupId(),
		Stream:   r.GetTopic(),
		Consumer: r.customer,
		MinIdle:  1 * time.Second,
		Messages: messageIds,
	})
	if err != nil {
		return nil, err
	}
	for _, v := range messages {
		if err = r.ConsumeMessage(v); err != nil {
			log.Errorf("process message: %v with err: %v", v, err)
			err = r.consumeErrMsg(v, err)
			if err != nil {
				log.Errorf("save failed message err: %v", err)
				continue
			}
		}
		err = r.redisClient.RelyAck(r.GetTopic(), r.GetGroupId(), v)
		if err != nil {
			log.Errorf("redis relay message err: %v", err)
		}
	}
	return pendingMessages, nil
}

func (r *BaseRedisCustomer) consumeMessageWithAck(message redis.XMessage) {
	r.wg.Add(1)
	defer func() {
		<-r.customerPool
		r.wg.Done()
	}()
	if err := r.ConsumeMessage(message); err != nil {
		log.Errorf("process message: %v with err: %v", message, err)
		err = r.consumeErrMsg(message, err)
		if err != nil {
			log.Errorf("save failed message err: %v", err)
			return
		}
	}
	err := r.redisClient.RelyAck(r.GetTopic(), r.GetGroupId(), message)
	if err != nil {
		log.Errorf("redis relay message %v ack err: %v", message, err)
	}
}

func (r *BaseRedisCustomer) StartConsume(ctx context.Context) {
	r.ConsumePendingMessage()
	go r.TrimQueueList(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			message, err := r.redisClient.ReadGroupMessages(r.GetTopic(), r.GetGroupId(), r.customer, r.msgCount)
			if err != nil {
				log.Errorf("redis fetch message err: %v", err)
				continue
			}
			for _, v := range message {
				r.customerPool <- struct{}{}
				go r.consumeMessageWithAck(v)
			}
		}

	}
}

func (r *BaseRedisCustomer) TrimQueueList(ctx context.Context) {
	r.wg.Add(1)
	defer func() {
		r.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streamLen, err := r.redisClient.StreamLen(r.GetTopic())
			if err != nil {
				log.Errorf("get topic %s lens err: %v", r.GetTopic(), err)
			}
			if streamLen < 1000 {
				time.Sleep(10 * time.Second)
				continue
			}
			info, err := r.redisClient.GetPendingInfo(r.GetTopic(), r.GetGroupId())
			if err != nil {
				log.Errorf("get topic %s group %s pending info err: %v", r.GetTopic(), r.GetGroupId(), err)
				continue
			}
			if info.Count == 0 {
				time.Sleep(5 * time.Second)
				continue
			}
			err = r.redisClient.TrimStream(r.GetTopic(), info.Lower)
			if err != nil {
				log.Errorf("trim topic %s failed: %v", r.GetTopic(), err)
				time.Sleep(5 * time.Second)
			}
		}
	}
}

func (r *BaseRedisCustomer) WaitClose() {
	r.wg.Wait()
}
