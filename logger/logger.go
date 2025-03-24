package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	// DefaultLevel the default log level
	DefaultLevel = zapcore.InfoLevel
	// DefaultTimeLayout the default time layout
	DefaultTimeLayout = time.RFC3339
)

// Option custom setup config
type Option func(*option)

type option struct {
	level          zapcore.Level
	fields         map[string]string
	file           io.Writer
	timeLayout     string
	disableConsole bool
}

var Logger *zap.Logger

// WithDebugLevel only greater than 'level' will output
func WithDebugLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.DebugLevel
	}
}

// WithInfoLevel only greater than 'level' will output
func WithInfoLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.InfoLevel
	}
}

// WithWarnLevel only greater than 'level' will output
func WithWarnLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.WarnLevel
	}
}

// WithErrorLevel only greater than 'level' will output
func WithErrorLevel() Option {
	return func(opt *option) {
		opt.level = zapcore.ErrorLevel
	}
}

// WithField add some field(s) to log
func WithField(key, value string) Option {
	return func(opt *option) {
		opt.fields[key] = value
	}
}

// WithFileP write log to some file
func WithFileP(file string) Option {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0766); err != nil {
		panic(err)
	}
	f, err := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		panic(err)
	}
	return func(opt *option) {
		opt.file = zapcore.Lock(f)
	}
}

// WithFileRotationP write log to some file with rotation
func WithFileRotationP(file string) Option {
	dir := filepath.Dir(file)
	if err := os.MkdirAll(dir, 0766); err != nil {
		panic(err)
	}
	return func(opt *option) {
		opt.file = &lumberjack.Logger{ // concurrent-saved
			Filename:   file, // 文件路径
			MaxSize:    128,  // 当个文件最大尺寸， 默认档位是 M
			MaxBackups: 300,  // 最多保留 300 个备份
			MaxAge:     30,   // 最大时间， 默认档位 day
			LocalTime:  true, // 使用本地时间
			Compress:   true, //是否压缩，disabled by default
		}
	}
}

// WithTimeLayout custom time format
func WithTimeLayout(timeLayout string) Option {
	return func(opt *option) {
		opt.timeLayout = timeLayout
	}
}

// WithDisableConsole  WithEnableConsole write log to os.Stdout or os.Stderr
func WithDisableConsole() Option {
	return func(opt *option) {
		opt.disableConsole = true
	}
}

// InitLogger
func InitLogger(opts ...Option) *zap.Logger {
	opt := &option{level: DefaultLevel, fields: make(map[string]string)}
	// 写法待理解
	for _, f := range opts {
		if f != nil {
			f(opt)
		}
	}

	timeLayout := DefaultTimeLayout
	if opt.timeLayout != "" {
		timeLayout = opt.timeLayout
	}
	// similar to zap.NewProductionEncoderConfig()
	encoderConfig := zapcore.EncoderConfig{
		MessageKey:    "msg",
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger", // used by Logger.Named(key); optional; useless
		CallerKey:     "caller",
		StacktraceKey: "stacktrace", // use by zap.AddStacktrace; optional; useless
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // 小写编码器
		EncodeTime: func(t time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(t.Format(timeLayout))
		},
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder, // 全路劲编码器
	}
	// lowPriority usd by info\debug\warn
	lowPriority := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
		return level >= opt.level && level < zapcore.ErrorLevel
	})
	// highPriority usd by error\panic\fatal
	highPriority := zap.LevelEnablerFunc(func(level zapcore.Level) bool {
		return level >= opt.level && level >= zapcore.ErrorLevel
	})

	stdout := zapcore.Lock(os.Stdout) // lock for concurrent safe
	stderr := zapcore.Lock(os.Stderr) // lock for concurrent safe

	core := zapcore.NewTee()
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	if !opt.disableConsole {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	core = zapcore.NewTee(
		zapcore.NewCore(encoder,
			zapcore.NewMultiWriteSyncer(stdout),
			lowPriority,
		),
		zapcore.NewCore(encoder,
			zapcore.NewMultiWriteSyncer(stderr),
			highPriority,
		),
	)

	if opt.file != nil {
		core = zapcore.NewTee(core,
			zapcore.NewCore(encoder,
				zapcore.AddSync(opt.file),
				zap.LevelEnablerFunc(func(level zapcore.Level) bool {
					return level >= opt.level
				}),
			),
		)
	}

	Logger = zap.New(core,
		zap.AddCaller(),
		zap.ErrorOutput(stderr),
	)

	for key, value := range opt.fields {
		Logger = Logger.WithOptions(zap.Fields(zapcore.Field{Key: key, Type: zapcore.StringType, String: value}))
	}
	return Logger
}

func GetLogger() *zap.Logger {
	return Logger
}

func setLogger() {
	if Logger == nil {
		Logger, _ = zap.NewProduction(zap.AddStacktrace(zapcore.LevelEnabler(zapcore.ErrorLevel)))
	}
}

func Info(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Info(msg, fields...)
}

func Debug(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Debug(msg, fields...)
}

func Warn(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Warn(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	setLogger()
	Logger.Error(msg, fields...)
}

func Sync() {
	err := Logger.Sync()
	if err != nil {
		return
	}
}

type Meta interface {
	Key() string
	Value() interface{}
	meta()
}

type meta struct {
	key   string
	value interface{}
}

func (m *meta) Key() string {
	return m.key
}

func (m *meta) Value() interface{} {
	return m.value
}

func (m *meta) meta() {}

func NewMeta(key string, value interface{}) Meta {
	return &meta{key: key, value: value}
}

func WrapMeta(err error, metas ...Meta) (fields []zap.Field) {
	capacity := len(metas) + 1 // message meta
	if err != nil {
		capacity++
	}
	fields = make([]zap.Field, 0, capacity)
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	fields = append(fields, zap.Namespace("meta"))
	for _, meta := range metas {
		fields = append(fields, zap.Any(meta.Key(), meta.Value()))
	}
	return
}
