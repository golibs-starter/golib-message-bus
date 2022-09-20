package log

import (
	kafkaConstant "gitlab.com/golibs-starter/golib-message-bus/kafka/constant"
	"gitlab.com/golibs-starter/golib/web/constant"
	webLog "gitlab.com/golibs-starter/golib/web/log"
)

func GetLoggingContext(metadata map[string]interface{}) []interface{} {
	loggingCtx := metadata[kafkaConstant.LoggingContext]
	if loggingCtx == nil {
		return nil
	}
	ctx, ok := loggingCtx.(*webLog.LoggingContext)
	if !ok {
		return nil
	}
	return []interface{}{constant.ContextReqMeta, ctx}
}
