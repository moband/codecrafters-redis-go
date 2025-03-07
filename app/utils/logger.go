package utils

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// LogLevel defines the level of logging
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarning
	LevelError
)

var (
	// Global logging state
	currentLogLevel           = LevelInfo
	logOutput       io.Writer = os.Stdout
	logMutex        sync.Mutex
	showTimestamps  = false
	timeFormat      = "2006-01-02 15:04:05"
)

// LogConfig configures the logger
type LogConfig struct {
	Level      LogLevel
	Writer     io.Writer
	ShowTime   bool
	TimeFormat string
}

// ConfigureLogger sets the global logging configuration
func ConfigureLogger(config LogConfig) {
	logMutex.Lock()
	defer logMutex.Unlock()

	if config.Level >= LevelDebug && config.Level <= LevelError {
		currentLogLevel = config.Level
	}

	if config.Writer != nil {
		logOutput = config.Writer
	}

	showTimestamps = config.ShowTime

	if config.TimeFormat != "" {
		timeFormat = config.TimeFormat
	}
}

// SetLogLevel sets the current log level
func SetLogLevel(level LogLevel) {
	logMutex.Lock()
	defer logMutex.Unlock()
	currentLogLevel = level
}

// Debug logs a debug message
func Debug(format string, args ...any) {
	if currentLogLevel <= LevelDebug {
		log("DEBUG", format, args...)
	}
}

// Info logs an info message
func Info(format string, args ...any) {
	if currentLogLevel <= LevelInfo {
		log("INFO", format, args...)
	}
}

// Warning logs a warning message
func Warning(format string, args ...any) {
	if currentLogLevel <= LevelWarning {
		log("WARNING", format, args...)
	}
}

// Error logs an error message
func Error(format string, args ...any) {
	if currentLogLevel <= LevelError {
		log("ERROR", format, args...)
	}
}

// log formats and prints a log message
func log(level, format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	logMutex.Lock()
	defer logMutex.Unlock()

	if showTimestamps {
		timestamp := time.Now().Format(timeFormat)
		fmt.Fprintf(logOutput, "[%s] [%s] %s\n", timestamp, level, message)
	} else {
		fmt.Fprintf(logOutput, "[%s] %s\n", level, message)
	}
}

// Logger provides structured logging for the application
type Logger struct {
	prefix string
	level  LogLevel
}

// NewLogger creates a new logger with the given prefix
func NewLogger(prefix string) *Logger {
	return &Logger{
		prefix: prefix,
		level:  currentLogLevel, // Use the global level by default
	}
}

// NewLoggerWithLevel creates a new logger with the given prefix and level
func NewLoggerWithLevel(prefix string, level LogLevel) *Logger {
	return &Logger{
		prefix: prefix,
		level:  level,
	}
}

// SetLevel sets the log level for this logger instance
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...any) {
	if l.level <= LevelDebug {
		l.log("DEBUG", format, args...)
	}
}

// Info logs an info message
func (l *Logger) Info(format string, args ...any) {
	if l.level <= LevelInfo {
		l.log("INFO", format, args...)
	}
}

// Warning logs a warning message
func (l *Logger) Warning(format string, args ...any) {
	if l.level <= LevelWarning {
		l.log("WARNING", format, args...)
	}
}

// Error logs an error message
func (l *Logger) Error(format string, args ...any) {
	if l.level <= LevelError {
		l.log("ERROR", format, args...)
	}
}

// log formats and prints a log message
func (l *Logger) log(level, format string, args ...any) {
	message := fmt.Sprintf(format, args...)

	logMutex.Lock()
	defer logMutex.Unlock()

	if showTimestamps {
		timestamp := time.Now().Format(timeFormat)
		fmt.Fprintf(logOutput, "[%s] [%s] %s: %s\n", timestamp, level, l.prefix, message)
	} else {
		fmt.Fprintf(logOutput, "[%s] %s: %s\n", level, l.prefix, message)
	}
}
