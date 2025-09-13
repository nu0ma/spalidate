package logging

import (
	stdlog "log"
	"os"
	"strings"

	chlog "github.com/charmbracelet/log"
	"github.com/muesli/termenv"
)

var defaultLogger *chlog.Logger

// colorMode: "auto" | "always" | "never"
func Init(level, format string, verbose bool, colorMode string) (func(), error) {
	l := chlog.NewWithOptions(os.Stderr, chlog.Options{ReportTimestamp: true})

	switch {
	case verbose:
		l.SetLevel(chlog.DebugLevel)
	default:
		switch strings.ToLower(level) {
		case "debug":
			l.SetLevel(chlog.DebugLevel)
		case "warn", "warning":
			l.SetLevel(chlog.WarnLevel)
		case "error":
			l.SetLevel(chlog.ErrorLevel)
		default:
			l.SetLevel(chlog.InfoLevel)
		}
	}
	if strings.EqualFold(format, "json") {
		l.SetFormatter(chlog.JSONFormatter)
	}
	// カラー強制/禁止（TextFormatter時のみ意味がある）
	if !strings.EqualFold(format, "json") {
		switch strings.ToLower(colorMode) {
		case "always":
			l.SetColorProfile(termenv.TrueColor)
		case "never":
			l.SetColorProfile(termenv.Ascii)
		}
	}

	prevWriter := stdlog.Writer()
	prevFlags := stdlog.Flags()
	prevPrefix := stdlog.Prefix()
	stdlog.SetFlags(0)
	stdlog.SetPrefix("")
	stdlog.SetOutput(&stdLogAdapter{L: l})

	defaultLogger = l

	cleanup := func() {
		stdlog.SetOutput(prevWriter)
		stdlog.SetFlags(prevFlags)
		stdlog.SetPrefix(prevPrefix)
	}
	return cleanup, nil
}

func L() *chlog.Logger {
	if defaultLogger == nil {
		defaultLogger = chlog.New(os.Stderr)
	}
	return defaultLogger
}

type stdLogAdapter struct{ L *chlog.Logger }

func (w *stdLogAdapter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\r\n")
	if msg != "" {
		w.L.Info(msg)
	}
	return len(p), nil
}
