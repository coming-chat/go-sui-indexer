package model

import (
	"fmt"
	log "github.com/golang/glog"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"strings"
	"time"
)

type Objects struct {
	ObjectId            string         `gorm:"primaryKey;not null;size:42;unique" json:"objectId"`
	Version             int64          `gorm:"type:bigint;not null;" json:"version"`
	Digest              string         `gorm:"not null;size:44;" json:"digest"`
	Owner               datatypes.JSON `gorm:"type:jsonb;" json:"owner"`
	PreviousTransaction string         `gorm:"size:44" json:"previousTransaction"`
	StorageRebate       int64          `gorm:"type:bigint" json:"storageRebate"`
	Status              string         `gorm:"size:10" json:"status"`
	DataType            string         `gorm:"size:255" json:"dataType"`
	Type                string         `gorm:"type:text;" json:"type"`
	HasPublicTransfer   bool           `gorm:"type:bool;" json:"hasPublicTransfer"`
	Fields              datatypes.JSON `gorm:"type:jsonb;" json:"fields"`
	CreateTime          time.Time      `gorm:"type:timestamp;not null;default:now()" json:"createTime"`
	UpdateTime          time.Time      `gorm:"type:timestamp;not null;default:now()" json:"updateTime"`
	IsUpdate            bool           `gorm:"type:bool;not null;default:false" json:"isUpdate"`
}

func (o Objects) TableName() string {
	return tablePrefix + "objects"
}

func (s *SuiDB) GetNeedUpdateObjects(tableName string, objects []*Objects) (result []*Objects) {
	var (
		tn           = Objects{}.TableName()
		objectIdList [][]interface{}
	)
	if tableName != "" {
		tn = tableName
	}

	for _, v := range objects {
		objectIdList = append(objectIdList, []interface{}{
			v.ObjectId, v.Version, v.Digest, v.UpdateTime,
		})
	}
	err := s.Table(tn).Where(
		"(object_id, version, digest, update_time) in ? and is_update = ? ",
		objectIdList, false,
	).Find(&result).Error
	if err != nil {
		log.Errorf("query objects: %+v err: %v", objects, err)
		return nil
	}
	return result
}

func (s *SuiDB) CreateObject(tableName string, object Objects) (*Objects, error) {
	var (
		tn     = Objects{}.TableName()
		result *Objects
	)
	if tableName != "" {
		tn = tableName
	}
	err := s.Table(tn).Create(&object).Error
	if err == nil {
		return &object, nil
	}
	if strings.Index(err.Error(), "duplicate key") == -1 {
		return nil, fmt.Errorf("process==> create objcet: %v failed with err: %v", object, err)
	}
	err = s.Table(tn).Where("object_id = ?", object.ObjectId).First(&result).Error
	if err != nil {
		return nil, fmt.Errorf("process==> query is_exists objcet: %v failed with err: %v", object, err)
	}
	if result.Version >= object.Version && result.IsUpdate == true {
		return nil, nil
	}
	return result, nil
}

func (s *SuiDB) UpdateObjects(tableName string, object Objects) bool {
	var tn = Objects{}.TableName()
	if tableName != "" {
		tn = tableName
	}
	result := s.Table(tn).Updates(&object)
	if result.Error != nil {
		log.Errorf("update object: %v failed with: %v", object, result.Error)
		return false
	}
	if result.RowsAffected > 0 {
		return true
	}
	return false
}

func (s *SuiDB) UpdateObjectToNeedUpdate(tableName string, object Objects) (*Objects, error) {
	var (
		tn     = Objects{}.TableName()
		result = &Objects{}
	)
	if tableName != "" {
		tn = tableName
	}
	update := s.Table(tn).Model(result).Clauses(clause.Returning{}).Where(
		"object_id = ? and version < ? and is_update = ?",
		object.ObjectId, object.Version, true,
	).Update("is_update", false)
	if update.Error != nil {
		return nil, fmt.Errorf("process==> update objcet: %v failed with err: %v", object, update.Error)
	}
	if update.RowsAffected > 0 {
		return result, nil
	}
	err := s.Table(tn).Where("object_id = ?", object.ObjectId).First(&result).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return s.CreateObject(tn, object)
		}
		return nil, err
	}
	if result.Version >= object.Version && result.IsUpdate == true {
		return nil, nil
	}
	return result, nil
}

func (s *SuiDB) GetLeaveOverNeedUpdateObjects(tableName string, from time.Time, limit, offerSet int) []*Objects {
	var (
		result []*Objects
		tn     = Objects{}.TableName()
	)
	if tableName != "" {
		tn = tableName
	}
	err := s.Table(tn).Where(
		"is_update = ? and update_time < ?",
		false, from,
	).Order("update_time ASC").Limit(limit).Offset(offerSet).Find(&result).Error
	if err != nil {
		log.Errorf("query which is_update = false failed: %v", err)
		return nil
	}
	return result
}

func (s *SuiDB) GetRelayMsg(objectType, relayObjectId, relayPoster string, errText []string) bool {
	var result []Objects
	queryData := s.Table(Objects{}.TableName()).Where(
		`type = ?
			and fields #>> '{"value","fields","poster"}' = ? 
			and fields #>> '{"value","fields","ref_id"}' = ? 
			and fields #>> '{"value","fields","text"}' not in ?`,
		objectType,
		relayPoster,
		relayObjectId,
		errText,
	).Find(&result)
	if queryData.Error != nil {
		log.Errorf("query db relay msg err: %v", queryData.Error)
		return false
	}

	if len(result) > 0 {
		return true
	}
	return false
}

func (s *SuiDB) DeleteObjects(tableName string, objectIds string) {
	tn := Objects{}.TableName()
	if tableName != "" {
		tn = tableName
	}
	result := s.Table(tn).Where("object_id = ?", objectIds).Delete(&Objects{})
	if result.Error != nil {
		log.Errorf("delete objects %v with err: %v", result.Error)
	}
}

func (s *SuiDB) GetObjectByIds(tableName string, objectIds ...string) ([]*Objects, error) {
	var object []*Objects
	tn := Objects{}.TableName()
	if tableName != "" {
		tn = tableName
	}
	err := s.Table(tn).Where("object_id in ?", objectIds).Find(&object).Error
	if err != nil {
		return nil, err
	}
	return object, nil
}
