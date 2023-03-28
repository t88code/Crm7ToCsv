package config

import (
	"gopkg.in/gcfg.v1"
	"io"
	"log"
	"os"
	"sync"
)

type Config struct {
	LOG struct {
		Debug bool
	}
	CRM7 struct {
		ConnectionString      string
		AccountTypeID         int
		ContactTypeIDFoodType int
		ContactTypeIDPpn      int
		QualifierIgnore       string
	}
	EXPORT struct {
		Path       string
		ExportDays int
		Date       string
		Mode       int
	}
	VERIFY struct {
		Verify           bool
		VeryfyErrorsSkip bool
		Path             string
	}
	REPORT struct {
		Report bool
		Path   string
	}
}

var cfg Config
var once sync.Once

func GetConfig() *Config {
	once.Do(func() {

		err := os.MkdirAll("logs", 0770)
		if err != nil {
			log.Fatal(err)
		}

		file, err := os.OpenFile("logs/config.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
		if err != nil {
			log.Fatal(err)
		}

		logger := log.New(io.MultiWriter(file, os.Stdout), "[ CONFIG ]", log.Ldate|log.Ltime|log.Lshortfile)
		//logger.Println("Read application configurations")

		// Read you config

		err = gcfg.ReadFileInto(&cfg, "./config.ini")
		if err != nil {
			logger.Fatalf("Config:>Failed to parse gcfg data: %s", err)
		} else {
			//logger.Print("Config:>Config is read")
		}
	})

	return &cfg
}
