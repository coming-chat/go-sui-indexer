package customer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/coming-chat/go-sui/client"
	"github.com/coming-chat/go-sui/types"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"time"
)

type TransactionCustomer struct {
	BaseRedisCustomer
	db       *model.SuiDB
	chainRpc *client.Client
	cache    *cache.Redis
	chain    string
	c        struct {
		BaseConfig  config.BaseConfig
		ObjectTopic string
	}
}

func NewTransactionCustomer(c config.Config, cc config.QueueConfig, db *model.SuiDB, cr *client.Client, rc *cache.Redis) *TransactionCustomer {
	t := &TransactionCustomer{
		db:                db,
		chainRpc:          cr,
		cache:             rc,
		chain:             fmt.Sprint(c.BaseConfig.Chain),
		BaseRedisCustomer: NewBaseRedisCustomer(cc, rc),
		c: struct {
			BaseConfig  config.BaseConfig
			ObjectTopic string
		}{
			BaseConfig:  c.BaseConfig,
			ObjectTopic: c.Queue[config.ObjectMsgQueue].RedisStreamConfig.Topic,
		},
	}
	t.consumeMsg = t.ConsumeRedisMessage
	t.consumeErrMsg = t.ProcessRedisFailedMessage
	return t
}

func (t *TransactionCustomer) ConsumeRedisMessage(message redis.XMessage) error {
	var (
		transactionDetail *types.TransactionResponse
		err               error
	)
	log.Infof("start fetch transaction: %v", message)
	txDigest := message.Values["data"].(string)

	for i := 0; i < t.c.BaseConfig.RequestRetryTime; i++ {
		transactionDetail, err = t.chainRpc.GetTransaction(context.Background(), txDigest)
		if err != nil {
			log.Errorf("fetch transaction detail err: %v", err)
		}
		if transactionDetail != nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if err != nil {
		return err
	}

	if transactionDetail == nil {
		return fmt.Errorf("get transaction digest %s failed", txDigest)
	}

	return t.processTransactions(
		&model.TransactionList{
			TransactionDigest: txDigest,
		},
		transactionDetail,
	)
}

func (t *TransactionCustomer) processTransactions(origin *model.TransactionList, detail *types.TransactionResponse) error {
	var (
		createObjects        []model.Objects
		updateObjects        []model.Objects
		isPackageTransaction = false
	)

	if detail.Certificate != nil && detail.Certificate.Data != nil && detail.Certificate.Data.Transactions != nil {
		for _, v := range detail.Certificate.Data.Transactions {
			if v.Call != nil && v.Call.Package.String() == t.c.BaseConfig.MovePackageConfig.PackageId {
				isPackageTransaction = true
				break
			}
		}
	}

	if isPackageTransaction {
		for _, c := range detail.Effects.Created {
			createObjects = append(createObjects, model.Objects{
				ObjectId:            c.Reference.ObjectId.String(),
				Version:             int64(c.Reference.Version),
				Digest:              c.Reference.Digest.String(),
				IsUpdate:            false,
				Status:              "Exists",
				PreviousTransaction: detail.Certificate.TransactionDigest,
			})
		}
		//Update
		for _, d := range detail.Effects.Deleted {
			updateObjects = append(updateObjects, model.Objects{
				ObjectId:            d.ObjectId.String(),
				Digest:              d.Digest.String(),
				Version:             int64(d.Version),
				IsUpdate:            false,
				Status:              "Exists",
				PreviousTransaction: detail.Certificate.TransactionDigest,
			})
		}
		for _, m := range detail.Effects.Mutated {
			updateObjects = append(updateObjects, model.Objects{
				ObjectId:            m.Reference.ObjectId.String(),
				Digest:              m.Reference.Digest.String(),
				Version:             int64(m.Reference.Version),
				IsUpdate:            false,
				Status:              "Exists",
				PreviousTransaction: detail.Certificate.TransactionDigest,
			})
		}
		for _, u := range detail.Effects.Unwrapped {
			updateObjects = append(updateObjects, model.Objects{
				ObjectId:            u.Reference.ObjectId.String(),
				Digest:              u.Reference.Digest.String(),
				Version:             int64(u.Reference.Version),
				IsUpdate:            false,
				Status:              "Exists",
				PreviousTransaction: detail.Certificate.TransactionDigest,
			})
		}
		for _, w := range detail.Effects.Wrapped {
			updateObjects = append(updateObjects, model.Objects{
				ObjectId:            w.ObjectId.String(),
				Digest:              w.Digest.String(),
				Version:             int64(w.Version),
				IsUpdate:            false,
				Status:              "Exists",
				PreviousTransaction: detail.Certificate.TransactionDigest,
			})
		}

		var (
			splitUpdateObjects [][]model.Objects
			groupUpdateObjects = new([]model.Objects)
		)
		//Create
		for _, v := range createObjects {
			cObject, err := t.db.CreateObject("", v)
			if err != nil {
				return err
			}

			if cObject == nil {
				continue
			}

			cObject.PreviousTransaction = ""
			cObject.Digest = ""
			cObject.DataType = ""
			cObject.Fields = nil
			cObject.Owner = nil
			cObject.Type = ""

			if len(*groupUpdateObjects) == t.c.BaseConfig.UpdateObjectBatchSize {
				splitUpdateObjects = append(splitUpdateObjects, *groupUpdateObjects)
				groupUpdateObjects = new([]model.Objects)
			}
			*groupUpdateObjects = append(*groupUpdateObjects, *cObject)
		}

		for _, v := range updateObjects {
			uObject, err := t.db.UpdateObjectToNeedUpdate("", v)
			if err != nil {
				return err
			}

			if uObject == nil {
				continue
			}

			uObject.PreviousTransaction = ""
			uObject.Digest = ""
			uObject.DataType = ""
			uObject.Fields = nil
			uObject.Owner = nil
			uObject.Type = ""

			if len(*groupUpdateObjects) == t.c.BaseConfig.UpdateObjectBatchSize {
				splitUpdateObjects = append(splitUpdateObjects, *groupUpdateObjects)
				groupUpdateObjects = new([]model.Objects)
			}

			*groupUpdateObjects = append(*groupUpdateObjects, *uObject)
		}

		if len(*groupUpdateObjects) != 0 {
			splitUpdateObjects = append(splitUpdateObjects, *groupUpdateObjects)
		}

		t.pushQueueMessage(splitUpdateObjects)
	}

	origin.TimestampMs = int64(detail.TimestampMs)
	origin.IsAnalyze = true
	_, err := t.db.CreateTransactions(origin)
	if err != nil && err != model.DuplicateKeyErr {
		return err
	}
	return nil
}

func (t *TransactionCustomer) pushQueueMessage(c [][]model.Objects) {
	for _, group := range c {
		vByte, err := json.Marshal(group)
		if err != nil {
			log.Errorf("push queue message: marshal objects json err: %v", err)
			continue
		}
		err = t.redisClient.PushMessageToStream(&redis.XAddArgs{
			Stream: t.c.ObjectTopic,
			Values: map[string]string{"data": string(vByte)},
		})
		if err != nil {
			log.Errorf("push message -> topic: %v err: %v", t.GetTopic(), err)
		}
	}
}

func (t *TransactionCustomer) ProcessRedisFailedMessage(message redis.XMessage, fromErr error) error {
	msg, err := json.Marshal(message.Values)
	if err != nil {
		return err
	}
	if !t.db.CreateQueueMessages([]model.QueueMessage{
		{
			Message:       msg,
			Key:           message.ID,
			Topic:         t.GetTopic(),
			GroupId:       t.GetGroupId(),
			ManualProcess: false,
			ErrReason:     fromErr.Error(),
			Status:        "failed",
		},
	}) {
		return errors.New("save failed message err")
	}
	return nil
}
