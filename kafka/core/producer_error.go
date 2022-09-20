package core

import "fmt"

// ProducerError is the type of error generated when the producer fails to deliver a message.
// It contains the original Message as well as the actual error value.
type ProducerError struct {
	Msg *Message
	Err error
}

func (pe ProducerError) Error() string {
	return fmt.Sprintf("Failed to produce message to topic %s: %s", pe.Msg.Topic, pe.Err)
}

func (pe ProducerError) Unwrap() error {
	return pe.Err
}
