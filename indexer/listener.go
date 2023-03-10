package indexer

import (
	"context"
	"fmt"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/coming-chat/go-sui/client"
	"github.com/coming-chat/go-sui/types"
	log "github.com/golang/glog"
	"time"
)

type (
	ListenLastTxByCycle struct {
		redisCache *cache.Redis
		chainRpc   *client.Client
		c          config.BaseConfig
		db         *model.SuiDB
		packageId  *types.ObjectId
	}

	ListenLastTxBySub struct {
		redisCache cache.Redis
		chainRpc   *client.Client
	}
)

func NewListenLastTxByCycle(c config.Config, rc *cache.Redis, cr *client.Client, db *model.SuiDB) *ListenLastTxByCycle {
	packageId, err := types.NewHexData(c.BaseConfig.MovePackageConfig.PackageId)
	if err != nil {
		panic(err)
	}
	return &ListenLastTxByCycle{
		redisCache: rc,
		chainRpc:   cr,
		c:          c.BaseConfig,
		packageId:  packageId,
		db:         db,
	}
}

func (l *ListenLastTxByCycle) ListenTx(ctx context.Context) <-chan TxDigest {
	tx := make(chan TxDigest, 1)
	go l.cycleFetchTransactionNum(ctx, tx)
	return tx
}

func (l *ListenLastTxByCycle) cycleFetchTransactionNum(ctx context.Context, tx chan<- TxDigest) {
	var (
		cursor     string
		lastDigest string
	)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var inputCursor *string = nil
			if lastDigest == "" {
				lastDigest = l.redisCache.GetChainPackageLastDigest(fmt.Sprint(l.c.Chain), l.c.MovePackageConfig.PackageId)
				if lastDigest == "" {
					lastDigest = l.db.GetLastTransactionDigest()
				}
			}
			if cursor != "" {
				inputCursor = &cursor
			} else if lastDigest != "" {
				inputCursor = &lastDigest
			}
			transactions, err := l.chainRpc.GetTransactions(
				context.Background(),
				types.TransactionQuery{
					InputObject: l.packageId,
				},
				inputCursor,
				l.c.FetchTxNum,
				false,
			)
			if err != nil {
				log.Errorf("sui_getTotalTransactions err: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if len(transactions.Data) == 0 {
				continue
			}

			for i, v := range transactions.Data {
				if i == 0 {
					switch v {
					case cursor:
						tx <- TxDigest{
							PreDigest: lastDigest,
							Digest:    v,
						}
					case lastDigest:
						// continue
					default:
						tx <- TxDigest{
							PreDigest: "",
							Digest:    v,
						}
					}
					continue
				}

				tx <- TxDigest{
					PreDigest: transactions.Data[i-1],
					Digest:    v,
				}
			}
			cursor = transactions.NextCursor
			lastDigest = transactions.Data[len(transactions.Data)-1]
			if transactions.NextCursor != "" {
				continue
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
