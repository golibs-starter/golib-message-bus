package core

// Producer publishes messages to the brokers
type Producer interface {

	// Send a message to the brokers
	Send(m *Message)

	// Close the producer
	Close()
}
