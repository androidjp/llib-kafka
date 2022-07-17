package llibkafka

import (
	"log"
	"os"
	"testing"
)

type testLogger struct {
	lg *log.Logger
}

func (l *testLogger) Debugf(format string, args ...interface{}) {
	l.lg.Printf(format, args...)
}

func (l *testLogger) Infof(format string, args ...interface{}) {
	l.lg.Printf(format, args...)
}

func (l *testLogger) Warnf(format string, args ...interface{}) {
	l.lg.Printf(format, args...)
}

func (l *testLogger) Errorf(format string, args ...interface{}) {
	l.lg.Printf(format, args...)
}

func TestSetLogger(t *testing.T) {
	lg := &testLogger{
		lg: log.New(os.Stderr, "", log.LstdFlags),
	}
	SetLogger(lg)

	logDebugf("test log debug")
	logInfof("test log info")
	logWarnf("test log warn")
	logErrorf("test log error")
}
