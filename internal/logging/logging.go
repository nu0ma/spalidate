package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Init(level, format string, verbose bool) (func(), error) {
	var cfg zap.Config
	if format == "json" {
		cfg = zap.NewProductionConfig()
	} else {
		cfg = zap.NewDevelopmentConfig()
	}

	var lvl zapcore.Level
	if verbose {
		lvl = zapcore.DebugLevel
	} else if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}
	cfg.Level = zap.NewAtomicLevelAt(lvl)

	logger, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	zap.ReplaceGlobals(logger)
	undo := zap.RedirectStdLog(logger)

	cleanup := func() {
		_ = logger.Sync()
		undo()
	}
	return cleanup, nil
}
