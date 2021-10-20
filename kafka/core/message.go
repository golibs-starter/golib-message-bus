package core

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
