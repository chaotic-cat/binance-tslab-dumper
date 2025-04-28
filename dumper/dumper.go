package dumper

import (
	"binanceklinedumper/formatter"
	"binanceklinedumper/util"
	"log"
	"os"
	"path"
	"time"
)

type Dumper struct {
	dataType  string
	symbol    string
	period    string
	fileName  string
	startDate time.Time
	endDate   time.Time
	formatter formatter.Formatter
}

func New(dataDir string, symbol string, dataType string, period string, startDate time.Time, endDate time.Time) *Dumper {
	if startDate.UnixMilli() < util.SymbolInfo[symbol].OnboardDate {
		startDate = time.UnixMilli(util.SymbolInfo[symbol].OnboardDate).UTC()
	}

	dataDir = path.Join(dataDir, dataType)
	os.MkdirAll(dataDir, os.ModePerm)
	formatter := formatter.New(dataType)
	if formatter == nil {
		log.Fatalln("invalid data type:", dataType)
	}

	return &Dumper{
		formatter: formatter,
		symbol:    symbol,
		period:    period,
		fileName:  formatter.GetFileName(dataDir, symbol, period),
		startDate: startDate,
		endDate:   endDate,
	}
}
