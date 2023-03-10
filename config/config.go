package config

import (
	"strconv"
)

const (
	TransactionMsgQueue = "TransactionMsgQueue"
	ObjectMsgQueue      = "ObjectMsgQueue"
)

type (
	QueueName string
	Config    struct {
		RedisConfig RedisConfig
		DBConfig    DBConfig
		BaseConfig  BaseConfig
		Queue       map[string]QueueConfig
	}

	BaseConfig struct {
		Mode                  string
		IsChainRest           bool
		IsContractUpdate      bool
		Chain                 Chain
		RequestRetryTime      int
		UpdateObjectBatchSize int
		FetchTxNum            uint
		MovePackageConfig     MovePackageConfig
	}

	MovePackageConfig struct {
		PackageId string
	}

	QueueConfig struct {
		Type              string
		Enabled           bool
		RedisStreamConfig RedisStreamConfig
	}

	RedisStreamConfig struct {
		Topic            string
		GroupID          string
		Customer         string
		CustomerQuantity int
		CustomerPool     int
		MsgCount         int64
	}

	Chain struct {
		ChainName string
		Network   string
		Rpc       string
	}

	RedisConfig struct {
		Address  string
		DB       int
		Password string
	}

	DBConfig struct {
		DBType   string
		Username string
		Password string
		Host     string
		DBName   string
		Port     int
	}
)

func (d DBConfig) String() string {
	return "host=" + d.Host + " port=" + strconv.Itoa(d.Port) + " user=" + d.Username + " password='" + d.Password +
		"' dbname=" + d.DBName + " sslmode=disable"
}

func (c Chain) String() string {
	return c.ChainName + "-" + c.Network
}
