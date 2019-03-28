package types

import (
	log "github.com/sirupsen/logrus"
	"os"
)

var Logrus *log.Logger

func init() {
	Logrus=log.New()
	Logrus.SetOutput(os.Stdout)
	Logrus.SetLevel(log.InfoLevel)
	Logrus.SetFormatter(&log.JSONFormatter{})
	Logrus.SetReportCaller(true)
}

func ChangeLogger(logger *log.Logger)  {
	if logger==nil {
		panic("log can not be nil")
	}
	Logrus=logger
}



