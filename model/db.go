package model

import (
	"errors"
	"fmt"
	"github.com/coming-chat/go-sui-indexer/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const tablePrefix = "sui_"

var DuplicateKeyErr = errors.New("record is exists")

type SuiDB struct {
	*gorm.DB
}

func NewSuiDB(c config.DBConfig) (*SuiDB, error) {
	dsn := fmt.Sprint(c)
	db, err := gorm.Open(postgres.Open(dsn))
	if err != nil {
		return &SuiDB{}, err
	}

	return &SuiDB{
		db,
	}, nil
}

func (s *SuiDB) Migrate() error {
	return s.AutoMigrate(&Objects{}, &QueueMessage{}, &TransactionList{})
}

func (s *SuiDB) ClearData() error {
	sql := `
	truncate table public.sui_queue_message restart identity;

	truncate table public.sui_transaction_list restart identity;

	truncate table public.sui_objects restart identity;
`
	return s.Exec(sql).Error
}

func (s *SuiDB) ClearPackageData() error {
	sql := `
	truncate table public.sui_objects restart identity;
`
	return s.Exec(sql).Error
}
