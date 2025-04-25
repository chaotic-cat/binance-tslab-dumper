package formatters

import (
	"context"
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

func Trades(ctx context.Context, csvReader *csv.Reader, writer *csv.Writer, lastDate time.Time) (time.Time, error) {
	for {
		select {
		case <-ctx.Done():
			return lastDate, ctx.Err()
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
		if len(row) < 6 {
			return lastDate, errors.Wrapf(err, "failed to parse row %q", row[0])
		}

		openTimeMs, err := strconv.ParseInt(row[4], 10, 64)
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
		formattedMsec := strings.Split(t.Format(time.StampMilli), ".")[1]
		id := row[0]
		price := row[1]
		var ask = "0.0"
		var askQTY = "0.0"
		var bid = "0.0"
		var bidQTY = "0.0"
		var oper string
		if strings.ToUpper(row[5]) == "TRUE" {
			bid = row[2]
			bidQTY = row[3]
			oper = "B"
		} else {
			ask = row[2]
			askQTY = row[3]
			oper = "S"
		}
		err = writer.Write([]string{formattedDate, formattedTime, formattedMsec, id, price, bid, bidQTY, ask, askQTY, oper})
		if err != nil {
			log.Fatalf("Error writing to CSV: %v", err)
		}
	}
	return lastDate, nil
}
