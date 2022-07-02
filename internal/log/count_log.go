package log

import (
	"go.uber.org/zap"
)

type CountLogger struct {
	countToLog int
	count      int
}

func NewCountLogger(countToLog int) *CountLogger {
	return &CountLogger{
		countToLog: countToLog,
		count:      0,
	}
}

func (c *CountLogger) Debug(msg string, fields ...zap.Field) {
	c.count++
	if c.count == c.countToLog {
		c.count = 0
		L().Debug(msg, fields...)
	}
}

func (c *CountLogger) Info(msg string, fields ...zap.Field) {
	c.count++
	if c.count == c.countToLog {
		c.count = 0
		L().Info(msg, fields...)
	}
}
