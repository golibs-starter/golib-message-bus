package core

import "context"

type Consumer interface {
	Start(ctx context.Context)
}

type ConsumerHandler interface {
	HandlerFunc(*ConsumerMessage)
	Close()
}
