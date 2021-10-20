package core

// AsyncProducer publishes messages to the brokers
type AsyncProducer interface {

	// Send a message to the brokers
	Send(m *Message)

	// Errors is the error output channel back to the user
	Errors() <-chan *ProducerError

	// Close the producer
	Close()
}
