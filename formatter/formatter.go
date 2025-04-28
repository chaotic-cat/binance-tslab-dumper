package formatter

import (
	"context"
	"encoding/csv"
	"os"
	"time"
)

type Formatter interface {
	GetFileName(dir string, symbol string, period string) string
	Write(ctx context.Context, symbol string, period string, csvReader *csv.Reader, writer *csv.Writer, lastDate time.Time) (time.Time, error)
	GetFileURL(symbol string, period string, timeRange string, dateStr string) (string, error)
	GetLastTimeWritten(row []string) (time.Time, error)
	WriteHeader(file *os.File) error
}

func New(dataType string) Formatter {
	switch dataType {
	case KlinesDataType:
		return &Klines{}
	case TradesDataType:
		return &Trades{}
	case MetricsDataType:
		return &Metrics{}
	}

	return nil
}
