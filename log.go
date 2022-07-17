package llibkafka

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

var logger Logger

func SetLogger(lg Logger) {
	if logger == nil {
		logger = lg
	}
}

func logDebugf(format string, args ...interface{}) {
	if logger != nil {
		logger.Debugf(format, args...)
	}
}

func logInfof(format string, args ...interface{}) {
	if logger != nil {
		logger.Infof(format, args...)
	}
}

func logWarnf(format string, args ...interface{}) {
	if logger != nil {
		logger.Warnf(format, args...)
	}
}

func logErrorf(format string, args ...interface{}) {
	if logger != nil {
		logger.Errorf(format, args...)
	}
}
