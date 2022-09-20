package handler

import (
	"context"
	"gitlab.com/golibs-starter/golib-message-bus/kafka/core"
)

func StartConsumers(consumer core.Consumer, ctx context.Context) {
	go consumer.Start(ctx)
}
