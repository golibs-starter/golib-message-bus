package core

import "context"

type Consumer interface {
	Start(ctx context.Context)
	Stop()
}

type ConsumerHandler interface {
	HandlerFunc(*ConsumerMessage)
	Close()
}
