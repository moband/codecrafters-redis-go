package replication

import (
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

// Logger for replication
var logger = utils.NewLogger("Replication")

// LogDebug logs a debug message
func LogDebug(format string, args ...interface{}) {
	logger.Debug(format, args...)
}

// LogInfo logs an info message
func LogInfo(format string, args ...interface{}) {
	logger.Info(format, args...)
}

// LogWarning logs a warning message
func LogWarning(format string, args ...interface{}) {
	logger.Warning(format, args...)
}

// LogError logs an error message
func LogError(format string, args ...interface{}) {
	logger.Error(format, args...)
}
