package logging

import (
	"os"
	"runtime/debug"
	"sync/atomic"
)

var (
	kLogger      = GetLogger("log")
	verboseLevel = int32(0)
)

func init() {
	kLogger.EnableRemote()
	kLogger.SetDepth(kLogger.Depth() + 1)
}

func Fatal(format string, args ...interface{}) {
	kLogger.Errorf(format, args...)
	kLogger.Errorf(string(debug.Stack()))
	kLogger.Flush()
	os.Exit(255)
}

func Assert(exp bool, format string, args ...interface{}) {
	if !exp {
		Fatal(format, args...)
	}
}

func InfoIf(flag bool, format string, args ...interface{}) {
	if flag {
		kLogger.Infof(format, args...)
	}
}

func Info(format string, args ...interface{}) {
	kLogger.Infof(format, args...)
}

func Warning(format string, args ...interface{}) {
	kLogger.Warningf(format, args...)
}

func Error(format string, args ...interface{}) {
	kLogger.Errorf(format, args...)
}

func Verbose(lvl int32, format string, args ...interface{}) {
	l := atomic.LoadInt32(&verboseLevel)
	if lvl <= l {
		kLogger.Infof(format, args...)
	}
}

func SetVerboseLevel(level int32) {
	Info("set verbose level to %d", level)
	atomic.StoreInt32(&verboseLevel, level)
}

func Flush() {
	kLogger.Flush()
}
