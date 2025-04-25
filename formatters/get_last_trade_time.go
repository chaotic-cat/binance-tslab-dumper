package formatters

import (
	"github.com/pkg/errors"
	"time"
)

func GetLastTradeTime(row []string, startDate time.Time, firstRow bool) (time.Time, error) {
	if len(row) < 10 {
		return time.Time{}, errors.New("invalid file format")
	}

	timeStr := row[0] + "," + row[1] + "," + row[2]
	tradeTime, err := time.Parse("20060102,150405,000", timeStr)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "Error parsing last kline time")
	}
	if firstRow && startDate.Before(tradeTime) && !startDate.Equal(tradeTime) && startDate.AddDate(0, 0, 1).Before(tradeTime) {
		return startDate, errors.New("there is no start date in this file")
	}

	return tradeTime, nil
}
