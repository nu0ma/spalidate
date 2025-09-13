package logging

import (
	stdlog "log"
	"os"

	chlog "github.com/charmbracelet/log"
)

var logger *chlog.Logger

func Init(verbose bool) (func(), error) {
	l := chlog.NewWithOptions(os.Stderr, chlog.Options{ReportTimestamp: true})
	if verbose {
		l.SetLevel(chlog.DebugLevel)
	}

	prevWriter := stdlog.Writer()
	prevFlags := stdlog.Flags()
	prevPrefix := stdlog.Prefix()
	stdlog.SetFlags(0)
	stdlog.SetPrefix("")

	logger = l

	cleanup := func() {
		stdlog.SetOutput(prevWriter)
		stdlog.SetFlags(prevFlags)
		stdlog.SetPrefix(prevPrefix)
	}
	return cleanup, nil
}

func L() *chlog.Logger {
	if logger == nil {
		logger = chlog.New(os.Stderr)
	}
	return logger
}
