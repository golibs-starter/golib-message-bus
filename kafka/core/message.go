package core

type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []MessageHeader
}

type MessageHeader struct {
	Key   []byte
	Value []byte
}
