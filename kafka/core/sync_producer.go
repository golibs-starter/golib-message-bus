package core

// SyncProducer publishes messages to the brokers
type SyncProducer interface {

	// Send a message to the brokers
	Send(m *Message) (partition int32, offset int64, err error)

	// Close the producer
	Close() error
}
