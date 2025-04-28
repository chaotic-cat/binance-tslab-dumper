package formatter

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const TradesDataType = "trades"

type Trades struct{}

func (t *Trades) WriteHeader(file *os.File) error {
	_, err := fmt.Fprintln(file, "<TICKER>,<DATE>,<TIME>,<MSEC>,<TRADENO>,<LAST>,<BID>,<BIDQTY>,<ASK>,<ASKQTY>,<OPER>")
	return err
}

func (t *Trades) GetLastTimeWritten(row []string) (time.Time, error) {
	if len(row) < 10 {
		return time.Time{}, errors.New("invalid file format")
	}

	timeStr := fmt.Sprintf("%s,%s,%s", row[1], row[2], row[3])
	tradeTime, err := time.Parse("20060102,150405,000", timeStr)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "Error parsing last kline time")
	}

	return tradeTime, nil
}

func (t *Trades) GetFileURL(symbol string, period string, timeRange string, dateStr string) (string, error) {
	fileURL := "https://data.binance.vision/data/futures/um"
	filePath := fmt.Sprintf("%s/trades/%s/%s-trades-%s.zip", timeRange, symbol, symbol, dateStr)

	var err error
	if fileURL, err = url.JoinPath(fileURL, filePath); err != nil {
		return "", errors.Wrapf(err, "failed to create the file URL from: %s and %s", fileURL, filePath)
	}
	return fileURL, nil
}

func (t *Trades) Write(ctx context.Context, symbol string, period string, csvReader *csv.Reader, writer *csv.Writer, lastDate time.Time) (time.Time, error) {
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
		date := t.Format("20060102")
		timestamp := t.Format("150405")
		msec := strings.Split(t.Format(time.StampMilli), ".")[1]
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
		err = writer.Write([]string{symbol, date, timestamp, msec, id, price, bid, bidQTY, ask, askQTY, oper})
		if err != nil {
			log.Fatalf("Error writing to CSV: %v", err)
		}
	}
	return lastDate, nil
}

func (t *Trades) GetFileName(dir string, symbol string, period string) string {
	return path.Join(dir, fmt.Sprintf("%s-%s.txt", symbol, TradesDataType))
}
