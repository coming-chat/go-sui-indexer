package customer

import (
	"context"
	"errors"
	log "github.com/golang/glog"
	"github.com/segmentio/kafka-go"
)

const KafkaFrameType = "Kafka"

type (
	KafkaCustomer interface {
		SubCustomer
		ConsumeKafkaMessage(message kafka.Message) error
		ProcessKafkaFailedMessage(message kafka.Message, fromErr error) error
	}

	BaseKafkaCustomer struct {
		brokers       []string
		partition     int
		groupId       string
		topic         string
		reader        *kafka.Reader
		consumeMsg    func(message kafka.Message) error
		consumeErrMsg func(message kafka.Message, fromErr error) error
	}
)

func (b *BaseKafkaCustomer) GetCustomerQuantity() int {
	return b.partition
}

func (b *BaseKafkaCustomer) GetGroupId() string {
	return b.groupId
}

func (b *BaseKafkaCustomer) GetTopic() string {
	return b.topic
}

func (b *BaseKafkaCustomer) GetCustomerType() string {
	return KafkaFrameType
}

func (b *BaseKafkaCustomer) InitCustomer() error {
	b.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  b.brokers,
		GroupID:  b.groupId,
		Topic:    b.topic,
		MaxBytes: 10e6,
	})
	return nil
}

func (b *BaseKafkaCustomer) StartConsume(ctx context.Context) {
	if b.reader == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			message, err := b.reader.FetchMessage(context.Background())
			if err != nil {
				log.Errorf("kafka fetch message err: %v", err)
				continue
			}
			if err = b.ConsumeMessage(message); err != nil {
				log.Errorf("process message: %v with err: %v", string(message.Key), err)
				err = b.consumeErrMsg(message, err)
				if err != nil {
					log.Errorf("save failed message err: %v", err)
					continue
				}
			}
			err = b.reader.CommitMessages(context.Background(), message)
			if err != nil {
				log.Errorf("kafka commit message err: %v", err)
			}
		}
	}

}

func (b *BaseKafkaCustomer) ConsumeMessage(message interface{}) error {
	kafkaMessage, ok := message.(kafka.Message)
	if !ok {
		return errors.New("this message not the kafka message")
	}
	return b.consumeMsg(kafkaMessage)
}

func (b *BaseKafkaCustomer) WaitClose() {
	return
}
