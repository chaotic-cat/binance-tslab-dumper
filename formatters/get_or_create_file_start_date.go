package formatters

import (
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"time"
)

func GetLastDate(symbol string, file *os.File, outputFile string, startDate time.Time, dataType string) (*os.File, time.Time, error) {
	var err error
	file, err = os.OpenFile(outputFile, os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, time.Time{}, err
	}
	csvReader := csv.NewReader(file)
	firstRow := true
	var lastTime time.Time
	// skip first row
	if _, err = csvReader.Read(); err != nil {
		return file, startDate, errors.Wrapf(err, "no first row in the file: %s", outputFile)
	}
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Error reading CSV row", symbol, err)
			continue
		}
		switch dataType {
		case "klines":
			lastTime, err = GetLastKlineTime(row, startDate, firstRow)
		case "trades":
			lastTime, err = GetLastTradeTime(row, startDate, firstRow)
		}
		if err != nil {
			return file, startDate, errors.Wrapf(err, "Invalid file format for %s. Can't read row properly", outputFile)
		}
		firstRow = false
	}
	if !lastTime.IsZero() && lastTime.AddDate(0, 0, 1).Month() != lastTime.Month() {
		lastTime = lastTime.AddDate(0, 0, 1).Truncate(24 * time.Hour)
	}

	return file, lastTime, nil
}
