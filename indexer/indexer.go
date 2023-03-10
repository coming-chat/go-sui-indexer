package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"sync"
	"time"
)

type (
	Indexer interface {
		Scan(ctx context.Context)
		WaitClose()
		Start(ctx context.Context)
	}

	ListenLatestNumber interface {
		ListenTx(ctx context.Context) <-chan TxDigest
	}

	SuiIndexer struct {
		l                     ListenLatestNumber
		c                     config.BaseConfig
		db                    *model.SuiDB
		redisCache            *cache.Redis
		wg                    sync.WaitGroup
		transactionQueueTopic string
		objectQueueTopic      string
	}

	TxDigest struct {
		PreDigest string
		Digest    string
	}
)

func NewSuiIndexer(c config.Config, l ListenLatestNumber, rc *cache.Redis, db *model.SuiDB) *SuiIndexer {
	return &SuiIndexer{
		l:                     l,
		c:                     c.BaseConfig,
		wg:                    sync.WaitGroup{},
		redisCache:            rc,
		db:                    db,
		transactionQueueTopic: c.Queue[config.TransactionMsgQueue].RedisStreamConfig.Topic,
		objectQueueTopic:      c.Queue[config.ObjectMsgQueue].RedisStreamConfig.Topic,
	}
}

func (s *SuiIndexer) Scan(ctx context.Context) {
	s.wg.Add(1)
	defer func() {
		s.wg.Done()
	}()
	txChannel := s.l.ListenTx(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case tx := <-txChannel:
			s.pushNewTxJob(tx)
		}
	}
}

func (s *SuiIndexer) Start(ctx context.Context) {
	go s.Scan(ctx)

	//go s.ProcessDropOutMessage(ctx)
}

func (s *SuiIndexer) pushNewTxJob(tx TxDigest) {
	s.redisCache.PushNewTxDigestToStream(s.transactionQueueTopic, fmt.Sprint(s.c.Chain), s.c.MovePackageConfig.PackageId, tx.PreDigest, tx.Digest)
}

func (s *SuiIndexer) WaitClose() {
	s.wg.Wait()
}

func (s *SuiIndexer) ProcessDropOutMessage(ctx context.Context) {
	s.wg.Add(1)
	defer func() {
		s.wg.Done()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var (
				querySize  = 1000
				objectsLen = querySize
				fromTime   = time.Now().UTC().Add(-10 * time.Minute)
			)
			for i := 0; objectsLen == querySize; i++ {
				objects := s.db.GetLeaveOverNeedUpdateObjects(
					"",
					fromTime,
					querySize,
					i*querySize,
				)
				objectsLen = len(objects)
				var pushObjects []*model.Objects
				for _, v := range objects {
					if len(pushObjects) < s.c.UpdateObjectBatchSize {
						pushObjects = append(pushObjects, v)
						continue
					}

					v.PreviousTransaction = ""
					v.Digest = ""
					v.DataType = ""
					v.Fields = nil
					v.Owner = nil
					v.Type = ""

					objectsB, err := json.Marshal(pushObjects)
					if err != nil {
						log.Errorf("marshal objects err: %v", err)
					}
					err = s.redisCache.PushMessageToStream(&redis.XAddArgs{
						Stream: s.objectQueueTopic,
						Values: map[string]string{"data": string(objectsB)},
					})
					if err != nil {
						log.Errorf("push message err: %v", err)
					}
					pushObjects = pushObjects[:0]
				}

			}
			time.Sleep(10 * time.Second)
		}
	}

}
