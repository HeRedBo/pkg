package logger

import (
	"errors"
	"go.uber.org/zap"
	"testing"
)

func TestLogger(t *testing.T) {
	logger := InitLogger()
	defer logger.Sync()
	err := errors.New("pkg error")
	logger.Error("err occurs", zap.Error(err))
	logger.Error("err occurs", WrapMeta(nil, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
	Info("this a info msg")

}
