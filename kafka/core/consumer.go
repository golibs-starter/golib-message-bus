package core

import "context"

type Consumer interface {
	Start(ctx context.Context)
	Close()
}

type ConsumerHandler interface {
	HandlerFunc(*ConsumerMessage)
	Close()
}
