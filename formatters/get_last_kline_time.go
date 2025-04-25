package formatters

import (
	"github.com/pkg/errors"
	"time"
)

func GetLastKlineTime(row []string, startDate time.Time, firstRow bool) (time.Time, error) {
	if len(row) < 9 {
		return startDate, errors.New("invalid file format")
	}

	timeStr := row[2] + "," + row[3]
	klineTime, err := time.Parse("20060102,150405", timeStr)
	if err != nil {
		return startDate, errors.Wrap(err, "Error parsing last kline time")
	}
	if firstRow && startDate.Before(klineTime) && !startDate.Equal(klineTime) {
		return startDate, errors.New("there is no last kline time in the file")
	}

	return klineTime, nil
}
