package dumper

import (
	"log"
	"os"
	"path"
	"time"

	"github.com/pkg/errors"

	"binance-tslab-dumper/formatter"
	"binance-tslab-dumper/util"
)

type Dumper struct {
	dataType       string
	symbol         string
	period         string
	fileName       string
	startDate      time.Time
	endDate        time.Time
	formatter      formatter.Formatter
	additionalType string
}

func New(dataDir string, symbol string, dataType string, period string, startDate time.Time, endDate time.Time, futures bool) *Dumper {
	if startDate.UnixMilli() < util.SymbolInfo[symbol].OnboardDate {
		startDate = time.UnixMilli(util.SymbolInfo[symbol].OnboardDate).UTC()
	}

	additionalType := "spot"
	if futures {
		additionalType = "futures"
	}

	dataDir = path.Join(dataDir, dataType, additionalType)
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		log.Fatal(errors.Wrapf(err, "failed to create directory %s", dataDir))
	}
	formatter := formatter.New(dataType)
	if formatter == nil {
		log.Fatalln("invalid data type:", dataType)
	}

	return &Dumper{
		dataType:       dataType,
		formatter:      formatter,
		symbol:         symbol,
		period:         period,
		fileName:       formatter.GetFileName(dataDir, symbol, period),
		startDate:      startDate,
		endDate:        endDate,
		additionalType: additionalType,
	}
}
