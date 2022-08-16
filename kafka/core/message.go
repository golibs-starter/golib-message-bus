package core

import (
	"fmt"
	"time"
)

type MessageHeader struct {
	Key   []byte
	Value []byte
}

func (m MessageHeader) String() string {
	return fmt.Sprintf("[%s: %s]", string(m.Key), string(m.Value))
}

type Message struct {
	Topic    string
	Key      []byte
	Value    []byte
	Headers  []MessageHeader
	Metadata interface{}
}

func (m Message) String() string {
	return fmt.Sprintf("[Topic: %s, Key: %s, Headers: %s, Payload: %s]",
		m.Topic, string(m.Key), m.Headers, string(m.Value))
}

type ConsumerMessage struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
	Partition int32
	Offset    int64
	Timestamp time.Time
}

func (m ConsumerMessage) String() string {
	return fmt.Sprintf("[Topic: %s, Partition: %d, Offset: %d, Timestamp: %s, Key: %s, Value: %s, Headers: %s]",
		m.Topic, m.Partition, m.Offset, m.Timestamp, string(m.Key), string(m.Value), m.Headers)
}
