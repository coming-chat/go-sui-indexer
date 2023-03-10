package model

import (
	"fmt"
	log "github.com/golang/glog"
	"gorm.io/gorm/clause"
	"strings"
	"time"
)

type TransactionList struct {
	Number            int64     `gorm:"primaryKey;type:bigint;not null;uniqueIndex;autoincrement" json:"number"`
	TransactionDigest string    `gorm:"size:44;not null;uniqueIndex" json:"transactionDigest"`
	IsAnalyze         bool      `gorm:"type:bool;not null" json:"isAnalyze"`
	CreateTime        time.Time `gorm:"type:timestamp;not null;default:now()" json:"createTime"`
	TimestampMs       int64     `gorm:"type:bigint" json:"timestampMs"`
	UpdateTime        time.Time `gorm:"type:timestamp;not null;default:now()" json:"updateTime"`
}

func (t TransactionList) TableName() string {
	return tablePrefix + "transaction_list"
}

func (s *SuiDB) GetTransactions(transactions []TransactionList) (result []*TransactionList) {
	var transaction [][]interface{}
	for _, v := range transactions {
		transaction = append(transaction, []interface{}{v.Number, v.TransactionDigest})
	}
	err := s.Model(&TransactionList{}).Where(
		"(number, transaction_digest) in ? and is_analyze = ? ",
		transaction, false,
	).Find(&result).Error
	if err != nil {
		log.Errorf("query transactions: %+v err: %v", transactions, err)
		return nil
	}
	return result
}

func (s *SuiDB) CreateTransactions(transactions *TransactionList) (*TransactionList, error) {
	err := s.Model(&TransactionList{}).Clauses(clause.Returning{}).Create(&transactions).Error
	if err == nil {
		return transactions, nil
	}
	if strings.Index(err.Error(), "duplicate key") == -1 {
		return nil, fmt.Errorf("create transactions: %+v err: %v", transactions, err)
	}

	result := s.Model(&TransactionList{}).Where(
		"transaction_digest = ? and number = ? and is_analyze = false",
		"unknown",
		transactions.Number,
	).Updates(&transactions)
	if result.Error != nil {
		return nil, result.Error
	}
	if result.RowsAffected > 0 {
		return transactions, nil
	}

	return nil, DuplicateKeyErr
}

func (s *SuiDB) UpdateTransaction(transaction TransactionList) bool {
	err := s.Model(&TransactionList{}).Where(
		"number = ? and transaction_digest = ? and is_analyze = ? and update_time = ?",
		transaction.Number,
		transaction.TransactionDigest,
		false,
		transaction.UpdateTime,
	).Updates(&transaction).Error
	if err != nil {
		log.Errorf("update transaction: %v failed with: %v", transaction, err)
		return false
	}
	return true
}

func (s *SuiDB) GetLastTransactionDigest() string {
	var digest string
	err := s.Model(&TransactionList{}).Select("transaction_digest").Order("timestamp_ms DESC").Limit(1).First(&digest).Error
	if err != nil {
		log.Errorf("get last transaction digest failed: %v", err)
	}
	return digest
}

func (s *SuiDB) GetDigestUnknownTransaction(size int) []*TransactionList {
	var result []*TransactionList
	err := s.Model(&TransactionList{}).Where(
		"transaction_digest = ?",
		"unknown",
	).Order("number ASC").Limit(size).Find(&result).Error
	if err != nil {
		log.Errorf("query digest unknown transaction err: %v", err)
	}
	return result
}

func (s *SuiDB) InsertStartTransaction(number int64) error {
	return s.Model(TransactionList{}).Create(&TransactionList{
		Number:            number,
		TransactionDigest: "start",
		IsAnalyze:         true,
	}).Error
}
