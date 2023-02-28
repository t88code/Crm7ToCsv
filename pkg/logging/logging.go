package logging

import (
	"Crm7ToCsv/pkg/config"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
)

var logMain = logrus.New()
var logTelegram = logrus.New()

type Logger struct {
	*logrus.Logger
}

func GetLogger() *Logger {
	return &Logger{
		Logger: logMain,
	}
}

func GetLoggerWithSeviceName(ServiceName string) *Logger {
	cfg := config.GetConfig()

	switch ServiceName {
	case "main":
		if logMain == nil {
			file, err := os.OpenFile("logs/main.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
			if err != nil {
				log.Fatal(err)
			}
			multiWriter := io.MultiWriter(file, os.Stdout)
			logMain.Out = multiWriter
			logMain.Formatter = &logrus.TextFormatter{
				ForceColors:               true,
				DisableColors:             false,
				ForceQuote:                false,
				DisableQuote:              false,
				EnvironmentOverrideColors: false,
				DisableTimestamp:          false,
				FullTimestamp:             true,
				TimestampFormat:           "2006-01-02 15:04:05",
				DisableSorting:            false,
				SortingFunc:               nil,
				DisableLevelTruncation:    false,
				PadLevelText:              false,
				QuoteEmptyFields:          false,
				FieldMap:                  nil,
				CallerPrettyfier:          nil,
			}
			if cfg.LOG.Debug {
				logMain.Level = logrus.DebugLevel
			} else {
				logMain.Level = logrus.InfoLevel
			}
		}
		return &Logger{
			Logger: logMain,
		}
	case "telegram":
		if logTelegram == nil {
			file, err := os.OpenFile("logs/telegram.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
			if err != nil {
				log.Fatal(err)
			}
			multiWriter := io.MultiWriter(file, os.Stdout)
			logTelegram.Out = multiWriter
			logTelegram.Formatter = &logrus.TextFormatter{
				ForceColors:               true,
				DisableColors:             false,
				ForceQuote:                false,
				DisableQuote:              false,
				EnvironmentOverrideColors: false,
				DisableTimestamp:          false,
				FullTimestamp:             true,
				TimestampFormat:           "2006-01-02 15:04:05",
				DisableSorting:            false,
				SortingFunc:               nil,
				DisableLevelTruncation:    false,
				PadLevelText:              false,
				QuoteEmptyFields:          false,
				FieldMap:                  nil,
				CallerPrettyfier:          nil,
			}
			if cfg.LOG.Debug {
				logTelegram.Level = logrus.DebugLevel
			} else {
				logTelegram.Level = logrus.InfoLevel
			}
		}
		return &Logger{
			Logger: logTelegram,
		}
	default:
		return nil
	}
}

func init() {
	err := os.MkdirAll("logs", 0770)
	if err != nil {
		log.Fatal(err)
	}

	cfg := config.GetConfig()
	file, err := os.OpenFile("logs/main.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		log.Fatal(err)
	}
	multiWriter := io.MultiWriter(file, os.Stdout)
	logMain.Out = multiWriter
	logMain.Formatter = &logrus.TextFormatter{
		ForceColors:               true,
		DisableColors:             false,
		ForceQuote:                false,
		DisableQuote:              false,
		EnvironmentOverrideColors: false,
		DisableTimestamp:          false,
		FullTimestamp:             true,
		TimestampFormat:           "2006-01-02 15:04:05",
		DisableSorting:            false,
		SortingFunc:               nil,
		DisableLevelTruncation:    false,
		PadLevelText:              false,
		QuoteEmptyFields:          false,
		FieldMap:                  nil,
		CallerPrettyfier:          nil,
	}
	if cfg.LOG.Debug {
		logMain.Level = logrus.DebugLevel
	} else {
		logMain.Level = logrus.InfoLevel
	}
}
