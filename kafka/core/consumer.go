package core

import "context"

type Consumer interface {
	Start(ctx context.Context)
	WaitForReady() chan bool
	Stop()
}

type ConsumerHandler interface {
	HandlerFunc(*ConsumerMessage)
	Close()
}
