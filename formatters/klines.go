package formatters

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"strconv"
	"time"
)

func Klines(ctx context.Context, csvReader *csv.Reader, symbol string, period string, writer *csv.Writer, lastDate time.Time) (time.Time, error) {
	for {
		select {
		case <-ctx.Done():
			return lastDate, nil
		default:
		}

		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV row: %v", err)
			continue
		}
		openTimeMs, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			log.Printf("Error parsing open_time: %v", err)
			continue
		}
		t := time.UnixMilli(openTimeMs).UTC()
		if !lastDate.IsZero() && !t.After(lastDate) {
			continue
		}
		lastDate = t
		formattedDate := t.Format("20060102")
		formattedTime := t.Format("150405")
		open := row[1]
		high := row[2]
		low := row[3]
		close_ := row[4]
		volume := row[5]
		err = writer.Write([]string{symbol, period, formattedDate, formattedTime, open, high, low, close_, volume})
		if err != nil {
			log.Printf("Error writing to CSV: %v", err)
		}
	}
	return lastDate, nil
}
