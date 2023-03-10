package customer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/coming-chat/go-sui/client"
	"github.com/coming-chat/go-sui/types"
	pgType "github.com/go-pg/pg/v10/types"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"time"
)

type ObjectCustomer struct {
	BaseRedisCustomer
	db       *model.SuiDB
	chainRpc *client.Client
	c        struct {
		BaseConfig config.BaseConfig
	}
	cache *cache.Redis
	chain string
}

func NewObjectCustomer(c config.Config, cc config.QueueConfig, db *model.SuiDB, cr *client.Client, rc *cache.Redis) *ObjectCustomer {
	o := &ObjectCustomer{
		db:                db,
		chainRpc:          cr,
		cache:             rc,
		chain:             fmt.Sprint(c.BaseConfig.Chain),
		BaseRedisCustomer: NewBaseRedisCustomer(cc, rc),
		c: struct {
			BaseConfig config.BaseConfig
		}{
			BaseConfig: c.BaseConfig,
		},
	}
	o.consumeMsg = o.ConsumeRedisMessage
	o.consumeErrMsg = o.ProcessRedisFailedMessage
	return o
}

func (o *ObjectCustomer) ConsumeRedisMessage(message redis.XMessage) error {
	var (
		objects        []*model.Objects
		objectId       []types.ObjectId
		objectIdString []string
		failedObject   []model.QueueMessage
	)
	log.Infof("start consume object message: %v", message)
	err := json.Unmarshal([]byte(message.Values["data"].(string)), &objects)
	if err != nil {
		return err
	}

	for _, v := range objects {
		hexObjectId, err := types.NewHexData(v.ObjectId)
		if err != nil {
			log.Errorf("object id to hex data err: %v", err)
			continue
		}
		objectId = append(objectId, *hexObjectId)
		objectIdString = append(objectIdString, v.ObjectId)
	}
	dbObjects, err := o.db.GetObjectByIds("", objectIdString...)
	if err != nil {
		return err
	}
	dbObjectsMap := make(map[string]*model.Objects)
	for _, v := range dbObjects {
		dbObjectsMap[v.ObjectId] = v
	}
	objectDetail := make(map[string]*types.ObjectRead)
	for i := 0; i < o.c.BaseConfig.RequestRetryTime; i++ {
		objectDetail, err = o.chainRpc.BatchGetObject(objectId)
		if err != nil {
			log.Errorf("fetch transaction detail err: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(objectDetail) != 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	for _, v := range objects {
		dbObject, ok := dbObjectsMap[v.ObjectId]
		if ok {
			err = o.processNewObject(dbObject, objectDetail)
		} else {
			err = o.processNewObject(v, objectDetail)
		}

		if err != nil {
			o.processFailedObject(&failedObject, []*model.Objects{v}, err)
		}
	}
	return nil
}

func (o *ObjectCustomer) processNewObject(v *model.Objects, objectDetail map[string]*types.ObjectRead) error {
	detail, ok := objectDetail[v.ObjectId]
	if !ok {
		return errors.New("not found this transaction detail in the fetch data")
	}

	switch detail.Status {
	case types.ObjectStatusNotExists:
		return errors.New("this object not exists")
	case types.ObjectStatusDeleted:
		//No things to do
	case types.ObjectStatusExists:
		v.Digest = detail.Details.Reference.Digest.String()
		v.Version = int64(detail.Details.Reference.Version)
		ownerJsonB, err := json.Marshal(detail.Details.Owner)
		if err != nil {
			return err
		}
		v.Owner = ownerJsonB

		if objectType, ok := detail.Details.Data["type"]; ok {
			v.Type = objectType.(string)
		}
		if objectDataType, ok := detail.Details.Data["dataType"]; ok {
			v.DataType = objectDataType.(string)
		}
		if hasPublicTransfer, ok := detail.Details.Data["has_public_transfer"]; ok {
			v.HasPublicTransfer = hasPublicTransfer.(bool)
		}
		if fields, ok := detail.Details.Data["fields"]; ok {
			fieldsJsonb, err := json.Marshal(fields)
			if err != nil {
				return err
			}
			fieldsJsonb = pgType.AppendJSONB(nil, fieldsJsonb, 0)
			v.Fields = fieldsJsonb
		}
		v.StorageRebate = int64(detail.Details.StorageRebate)
		v.PreviousTransaction = detail.Details.PreviousTransaction
	default:
		return fmt.Errorf("unknown status: %v", detail.Status)
	}
	v.Status = string(detail.Status)
	v.IsUpdate = true

	if !o.db.UpdateObjects("", *v) {
		return errors.New("save object data err")
	}

	return nil
}

func (o *ObjectCustomer) processFailedObject(failedList *[]model.QueueMessage, object []*model.Objects, fromErr error) {
	data := make(map[string]interface{})
	data["data"] = object
	msg, err := json.Marshal(data)
	if err != nil {
		log.Errorf("process failed object marshal jsonb err: %v", err)
		return
	}

	*failedList = append(*failedList, model.QueueMessage{
		Message:       msg,
		Key:           object[0].ObjectId,
		GroupId:       o.GetGroupId(),
		Topic:         o.GetTopic(),
		Status:        "failed",
		ManualProcess: false,
		ErrReason:     fromErr.Error(),
	})
}

func (o *ObjectCustomer) ProcessRedisFailedMessage(message redis.XMessage, fromErr error) error {
	msg, err := json.Marshal(message.Values)
	if err != nil {
		return err
	}
	if !o.db.CreateQueueMessages([]model.QueueMessage{
		{
			Message:       msg,
			Key:           message.ID,
			Topic:         o.GetTopic(),
			ManualProcess: false,
			ErrReason:     fromErr.Error(),
			Status:        "failed",
			GroupId:       o.GetGroupId(),
		},
	}) {
		return errors.New("save failed message err")
	}
	return nil
}
