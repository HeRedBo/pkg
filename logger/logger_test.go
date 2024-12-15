package logger

import (
	"testing"
)

func TestLogger(t *testing.T) {
	//logger := GetLogger()
	//defer logger.Sync()
	//err = errors.New("pkg error")
	//logger.Error("err occurs", WrapMeta(nil, NewMeta("para1", "value1"), NewMeta("para2", "value2"))...)
	Info("this a info msg")

}
