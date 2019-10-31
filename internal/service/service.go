package service

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/syncromatics/kafkey/internal/kafkey"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Service is the implementation of the kafkey service
type Service struct {
	broker   string
	registry string
}

// New creates a new kafkey service
func New(broker string, registry string) *Service {
	return &Service{broker, registry}
}

func (s *Service) Watch(request *kafkey.WatchRequest, stream kafkey.Kafkey_WatchServer) error {
	topic := request.Topic
	key := request.Key

	registry, err := NewRegistry(s.registry)
	if err != nil {
		return errors.Wrap(err, "failed to create registry")
	}

	offset := ""
	switch request.Offset {
	case kafkey.Offset_EARLIEST:
		offset = "earliest"
	case kafkey.Offset_LATEST:
		offset = "latest"
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  s.broker,
		"auto.offset.reset":  offset,
		"group.id":           "kafkey",
		"enable.auto.commit": false,
	})
	if err != nil {
		return errors.Wrap(err, "failed to create consumer")
	}
	defer c.Close()

	c.SubscribeTopics([]string{topic}, nil)

	template := "{\"timestamp\":\"%s\",\"value\":%s}"
	for {
		msg, err := c.ReadMessage(-1)
		if string(msg.Key) != key {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "failed reading message")
		}

		m, err := registry.Decode(msg.Value)
		if err != nil {
			return errors.Wrap(err, "failed decoding message")
		}

		err = stream.Send(&kafkey.WatchResponse{
			Message: fmt.Sprintf(template, msg.Timestamp, m),
		})
		if err != nil {
			return errors.Wrap(err, "failed sending message")
		}
	}
}
