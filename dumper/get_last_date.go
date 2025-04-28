package dumper

import (
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"time"
)

func (d *Dumper) getLastDate(startDate time.Time) (time.Time, error) {
	lastDate, err := d.readLastDate(d.fileName, startDate)
	if err != nil {
		return time.Time{}, err
	}
	if lastDate.IsZero() {
		return time.Time{}, errors.New("no last date")
	}

	nextMonth := lastDate.AddDate(0, 0, 1).Month()
	if !lastDate.IsZero() && nextMonth != lastDate.Month() {
		lastDate = lastDate.AddDate(0, 0, 1).Truncate(24 * time.Hour)
	}

	return lastDate, nil
}

func (d *Dumper) readLastDate(fileName string, startDate time.Time) (time.Time, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDONLY, os.ModePerm)
	if err != nil {
		return startDate, err
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	var lastTime time.Time
	// skip header
	if _, err = csvReader.Read(); err != nil {
		return startDate, errors.Wrapf(err, "no first row in the file: %s", fileName)
	}
	var temp time.Time
	for {
		temp, err = d.readDate(csvReader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return time.Time{}, err
		}
		lastTime = temp
	}

	return lastTime, nil
}

func (d *Dumper) readDate(csvReader *csv.Reader) (time.Time, error) {
	row, err := csvReader.Read()
	if err == io.EOF {
		return time.Time{}, err
	}
	if err != nil {
		log.Println("Error reading CSV row", d.symbol, err)
	}
	lastTime, err := d.formatter.GetLastTimeWritten(row)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "Invalid file format. Can't read row properly")
	}
	return lastTime, nil
}
