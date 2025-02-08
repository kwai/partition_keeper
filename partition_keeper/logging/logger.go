package logging

type Logger interface {
	EnableRemote()
	Depth() int
	SetDepth(depth int)
	Errorf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Flush()
}

type LoggerFactory func(moduleName string) Logger

var (
	loggerFactory LoggerFactory = GetDefaultLogger
)

func SetLoggerFactory(factory LoggerFactory) {
	loggerFactory = factory
}

func GetLogger(moduleName string) Logger {
	return loggerFactory(moduleName)
}
