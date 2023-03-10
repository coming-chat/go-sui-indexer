package node

import (
	"context"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/coming-chat/go-sui-indexer/customer"
	"github.com/coming-chat/go-sui-indexer/indexer"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/coming-chat/go-sui/client"
	log "github.com/golang/glog"
)

type Node struct {
	indexer  indexer.Indexer
	customer customer.Customer
	suiDb    *model.SuiDB
	cache    *cache.Redis
	chain    *client.Client
	c        config.Config
	ctx      context.Context
}

func NewNode(c config.Config, ctx context.Context, db *model.SuiDB, cache *cache.Redis, chain *client.Client) *Node {
	return &Node{
		c:        c,
		ctx:      ctx,
		suiDb:    db,
		cache:    cache,
		chain:    chain,
		customer: customer.NewCustomer(ctx, db, cache),
		indexer: indexer.NewSuiIndexer(
			c,
			indexer.NewListenLastTxByCycle(c, cache, chain, db),
			cache,
			db,
		),
	}
}

func (n *Node) Init() {
	if n.c.BaseConfig.IsChainRest {
		err := n.ResetChain()
		if err != nil {
			panic(err)
		}
	} else if n.c.BaseConfig.IsContractUpdate {
		err := n.ResetPackage()
		if err != nil {
			panic(err)
		}
	}
	var subCustomers []customer.SubCustomer
	for k, v := range n.c.Queue {
		if !v.Enabled {
			continue
		}
		var newCustomer customer.SubCustomer
		switch k {
		case config.TransactionMsgQueue:
			newCustomer = customer.NewTransactionCustomer(n.c, n.c.Queue[k], n.suiDb, n.chain, n.cache)
		case config.ObjectMsgQueue:
			newCustomer = customer.NewObjectCustomer(n.c, n.c.Queue[k], n.suiDb, n.chain, n.cache)
		default:
			panic("no match new queue func for " + k)
		}
		subCustomers = append(subCustomers, newCustomer)
	}
	n.customer.RegisterCustomer(subCustomers...)
	n.customer.InitCustomers()
}

func (n *Node) Close() {
	n.indexer.WaitClose()
	n.customer.WaitClose()
}

func (n *Node) Start() {
	log.Infoln("<=========== start indexer =============>")
	n.indexer.Start(n.ctx)

	log.Infoln("<=========== start customer ============>")
	n.customer.StartConsume()
}
