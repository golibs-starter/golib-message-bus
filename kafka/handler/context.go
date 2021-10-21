package handler

import (
	kafkaConstant "gitlab.id.vin/vincart/golib-message-bus/kafka/constant"
	webLog "gitlab.id.vin/vincart/golib/web/log"
)

func getLoggingContext(metadata map[string]interface{}) *webLog.LoggingContext {
	loggingCtx := metadata[kafkaConstant.LoggingContext]
	if loggingCtx == nil {
		return nil
	}
	ctx, ok := loggingCtx.(*webLog.LoggingContext)
	if !ok {
		return nil
	}
	return ctx
}
