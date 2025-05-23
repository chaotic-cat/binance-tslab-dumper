package dumper

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"time"

	"github.com/pkg/errors"
)

func (d *Dumper) getLastData(startDate time.Time) (time.Time, int64, error) {
	lastDate, lastTrade, err := d.readLastDate(d.fileName, startDate)
	if err != nil {
		return time.Time{}, 0, err
	}
	if lastDate.IsZero() {
		return time.Time{}, 0, errors.New("no last date")
	}

	return lastDate, lastTrade, nil
}

func (d *Dumper) readLastDate(fileName string, startDate time.Time) (time.Time, int64, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return startDate, -1, err
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	var lastTime time.Time
	// skip header
	if _, err = csvReader.Read(); err != nil {
		return startDate, -1, errors.Wrapf(err, "no first row in the file: %s", fileName)
	}
	var lastTrade int64
	for {
		temp, tempTrade, err := d.readData(csvReader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return time.Time{}, -1, err
		}
		lastTime = temp
		lastTrade = tempTrade
	}

	return lastTime, lastTrade, nil
}

func (d *Dumper) readData(csvReader *csv.Reader) (time.Time, int64, error) {
	row, err := csvReader.Read()
	if err == io.EOF {
		return time.Time{}, 0, err
	}
	if err != nil {
		log.Println("Error reading CSV row", d.symbol, err)
	}
	lastTime, lastTrade, err := d.formatter.GetLastTimeWritten(row)
	if err != nil {
		return time.Time{}, 0, errors.Wrapf(err, "Invalid file format. Can't read row properly")
	}
	return lastTime, lastTrade, nil
}
