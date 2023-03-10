package node

import (
	"fmt"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
)

func (n *Node) ResetChain() error {
	var keys = []string{fmt.Sprintf(cache.PrefixChainLastDigest, n.c.BaseConfig.Chain, n.c.BaseConfig.MovePackageConfig.PackageId)}
	for _, v := range n.c.Queue {
		if !v.Enabled {
			continue
		}
		keys = append(keys, v.RedisStreamConfig.Topic)
	}
	err := n.cache.ClearKey(keys...)
	if err != nil {
		return err
	}
	err = n.suiDb.ClearData()
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) ResetPackage() error {
	var keys []string
	for k, v := range n.c.Queue {
		if !v.Enabled || k == config.TransactionMsgQueue {
			continue
		}
		keys = append(keys, v.RedisStreamConfig.Topic)
	}
	err := n.cache.ClearKey(keys...)
	if err != nil {
		return err
	}
	err = n.suiDb.ClearPackageData()
	if err != nil {
		return err
	}
	return nil
}
