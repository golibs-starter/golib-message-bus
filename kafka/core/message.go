package core

import "fmt"

type Message struct {
	Topic    string
	Key      []byte
	Value    []byte
	Headers  []MessageHeader
	Metadata interface{}
}

type MessageHeader struct {
	Key   []byte
	Value []byte
}

func (m MessageHeader) String() string {
	return fmt.Sprintf("[Key: %s, Value: %s]", string(m.Key), string(m.Value))
}
