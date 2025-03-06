package utils

import "fmt"

// LogLevel defines the level of logging
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelError
)

var currentLogLevel = LevelInfo

// Logger provides structured logging for the application
type Logger struct {
	prefix string
}

// NewLogger creates a new logger with the given prefix
func NewLogger(prefix string) *Logger {
	return &Logger{
		prefix: prefix,
	}
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	if currentLogLevel <= LevelDebug {
		l.log("DEBUG", format, args...)
	}
}

// Info logs an info message
func (l *Logger) Info(format string, args ...interface{}) {
	if currentLogLevel <= LevelInfo {
		l.log("INFO", format, args...)
	}
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	if currentLogLevel <= LevelError {
		l.log("ERROR", format, args...)
	}
}

// log formats and prints a log message
func (l *Logger) log(level, format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] %s: %s\n", level, l.prefix, message)
}
