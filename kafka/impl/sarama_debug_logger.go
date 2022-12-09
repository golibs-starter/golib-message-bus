package impl

import "gitlab.com/golibs-starter/golib/log"

type DebugLogger struct {
}

func NewDebugLogger() *DebugLogger {
	return &DebugLogger{}
}

func (l DebugLogger) Print(v ...interface{}) {
	log.Debug(v...)
}

func (l DebugLogger) Printf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func (l DebugLogger) Println(v ...interface{}) {
	log.Debug(v...)
}
