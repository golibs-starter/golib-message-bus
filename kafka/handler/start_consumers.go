package handler

import (
	"context"
	"gitlab.id.vin/vincart/golib-message-bus/kafka/core"
)

func StartConsumers(consumer core.Consumer, ctx context.Context) {
	consumer.Start(ctx)
}
