package logging

import (
	"fmt"
	"sync"
)

type defaultLogger struct {
	mu    sync.RWMutex
	depth int
}

func GetDefaultLogger(moduleName string) Logger {
	return &defaultLogger{}
}

func (l *defaultLogger) EnableRemote() {}

func (l *defaultLogger) Depth() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.depth
}

func (l *defaultLogger) SetDepth(depth int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.depth = depth
}

func (l *defaultLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Printf("\n")
}

func (l *defaultLogger) Warningf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Printf("\n")
}

func (l *defaultLogger) Infof(format string, args ...interface{}) {
	fmt.Printf(format, args...)
	fmt.Printf("\n")
}

func (l *defaultLogger) Flush() {
}
