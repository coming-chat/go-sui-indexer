package main

import (
	"context"
	"flag"
	"github.com/coming-chat/go-sui-indexer/cache"
	"github.com/coming-chat/go-sui-indexer/config"
	"github.com/coming-chat/go-sui-indexer/model"
	"github.com/coming-chat/go-sui-indexer/node"
	"github.com/coming-chat/go-sui/client"
	"github.com/go-redis/redis/v8"
	log "github.com/golang/glog"
	"github.com/zeromicro/go-zero/core/conf"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var configFile = flag.String("f", "etc/sui-indexer.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c, conf.UseEnv())

	rc := redis.NewClient(&redis.Options{
		Addr:     c.RedisConfig.Address,
		Password: c.RedisConfig.Password,
		DB:       c.RedisConfig.DB,
	})
	_, err := rc.Ping(rc.Context()).Result()
	if err != nil {
		log.Fatal("connect redis failed: ", err)
	}
	crc, err := cache.NewRedis(rc)
	if err != nil {
		log.Fatal("init redis failed: ", err)
	}

	hc := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:    20,
			IdleConnTimeout: 20 * time.Second,
		},
		Timeout: 15 * time.Second,
	}
	ct, _ := client.DialWithClient(c.BaseConfig.Chain.Rpc, hc)

	suiDb, err := model.NewSuiDB(c.DBConfig)
	if err != nil {
		log.Fatal("init db failed: ", err)
	}
	err = suiDb.Migrate()
	if err != nil {
		log.Fatal("migrate db failed: ", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	indexerNode := node.NewNode(c, ctx, suiDb, crc, ct)
	indexerNode.Init()
	indexerNode.Start()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	select {
	case <-interrupt:
		log.Infoln("[closing] signal get, start close job")
	}

	cancel()

	shutdownFinish := make(chan struct{})
	timeWait := time.After(120 * time.Second)

	go func() {
		indexerNode.Close()
		conn, err := suiDb.DB.DB()
		if err == nil {
			err = conn.Close()
			if err != nil {
				log.Errorf("DB close error: %s", err)
			} else {
				log.Info("db closed")
			}
		}
		crc.Close()
		shutdownFinish <- struct{}{}
	}()

	select {
	case <-shutdownFinish:
		log.Infoln("closed: all goroutine finished")
	case <-timeWait:
		log.Infoln("closed: timeout")
	}

}
