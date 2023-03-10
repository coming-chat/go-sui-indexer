package customer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"sync"
	"time"
)

type (
	SubCustomer interface {
		InitCustomer() error
		GetCustomerType() string
		GetTopic() string
		GetGroupId() string
		GetCustomerQuantity() int
		WaitClose()

		ConsumeMessage(message interface{}) error
		StartConsume(ctx context.Context)
	}

	Customer struct {
		wg        sync.WaitGroup
		db        *model.SuiDB
		ctx       context.Context
		cache     *cache.Redis
		customers map[string]SubCustomer
	}
)

func NewCustomer(ctx context.Context, db *model.SuiDB, cr *cache.Redis) Customer {
	return Customer{
		wg:        sync.WaitGroup{},
		db:        db,
		ctx:       ctx,
		cache:     cr,
		customers: make(map[string]SubCustomer),
	}
}

func (c *Customer) RegisterCustomer(customers ...SubCustomer) {
	for _, v := range customers {
		c.customers[v.GetTopic()] = v
	}
}

func (c *Customer) StartConsume() {
	for _, v := range c.customers {
		for i := 0; i < v.GetCustomerQuantity(); i++ {
			go v.StartConsume(c.ctx)
		}
	}

	go c.ProcessFailedMessage()
}

func (c *Customer) InitCustomers() {
	for _, v := range c.customers {
		if err := v.InitCustomer(); err != nil {
			panic(fmt.Sprintf("init customer group %s topic %s failed with: %v", v.GetGroupId(), v.GetTopic(), err))
		}

	}
}

func (c *Customer) WaitClose() {
	for _, v := range c.customers {
		v.WaitClose()
	}
	c.wg.Wait()
}

func (c *Customer) ProcessFailedMessage() {
	c.wg.Add(1)
	defer func() {
		c.wg.Done()
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			failedQueueMessage := c.db.GetQueueMessage(10)
			if len(failedQueueMessage) == 0 {
				time.Sleep(2 * time.Second)
			}
			for _, v := range failedQueueMessage {
				if !c.cache.SetQueueConsumeLock(v.Id) {
					continue
				}
				if v.AutoCustomCount > 3 {
					v.ManualProcess = true
					c.db.UpdateQueueMessage(v)
					continue
				}
				v.AutoCustomCount++
				customer, ok := c.customers[v.Topic]
				if !ok {
					log.Errorf("not found topic: %s -> customer", v.Topic)
					continue
				}
				message, _ := v.Message.MarshalJSON()
				msg := redis.XMessage{
					ID:     fmt.Sprintf("failed message id: %d", v.Id),
					Values: make(map[string]interface{}),
				}
				err := json.Unmarshal(message, &msg.Values)
				if err != nil {
					log.Errorf("unmarshal json failed message: %s failed: %v", v.Message.String(), err)
					v.ErrReason = err.Error()
					v.AutoCustomCount++
					c.db.UpdateQueueMessage(v)
					continue
				}
				err = customer.ConsumeMessage(msg)
				if err != nil {
					log.Errorf("custom failed message: %s failed again: %v", v.Message.String(), err)
					v.ErrReason = err.Error()
					c.db.UpdateQueueMessage(v)
					continue
				}
				v.Status = "success"
				c.db.UpdateQueueMessage(v)
				c.cache.UnlockQueueConsumeLock(v.Id)
			}
		}
	}
}
