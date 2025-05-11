package formatter

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const TradesDataType = "trades"

type Trades struct{}

func (t *Trades) WriteHeader(file *os.File) error {
	//<TICKER>,<DATE>,<TIME>,<MSEC>,<TRADENO>,<LAST>,<OPER>
	_, err := fmt.Fprintln(file, "<TICKER>,<DATE>,<TIME>,<MSEC>,<TRADENO>,<LAST>,<VOL>,<OPER>")
	return err
}

func (t *Trades) GetLastTimeWritten(row []string) (time.Time, int64, error) {
	if len(row) < 8 {
		return time.Time{}, 0, errors.New("invalid file format")
	}

	timeStr := fmt.Sprintf("%s,%s,%s", row[1], row[2], row[3])
	tradeTime, err := time.Parse("20060102,150405,000", timeStr)
	if err != nil {
		return time.Time{}, 0, errors.Wrap(err, "Error parsing last trade time")
	}
	lastTrade, err := strconv.ParseInt(row[4], 10, 64)
	if err != nil {
		return time.Time{}, 0, errors.Wrap(err, "Error parsing last trade id")
	}

	return tradeTime, lastTrade, nil
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

func (t *Trades) Write(ctx context.Context, symbol string, _ string, csvReader *csv.Reader, writer *csv.Writer, lastWriteData time.Time, lastTradeID int64) (time.Time, int64, error) {
	for {
		select {
		case <-ctx.Done():
			return lastWriteData, lastTradeID, nil
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
			return lastWriteData, lastTradeID, errors.Wrapf(err, "failed to parse row %q", row[0])
		}

		openTimeMs, err := strconv.ParseInt(row[4], 10, 64)
		if err != nil {
			log.Printf("Error parsing open_time: %v", err)
			continue
		}

		t := time.UnixMilli(openTimeMs).UTC()
		lastWriteData = t
		date := t.Format("20060102")
		timestamp := t.Format("150405")
		msec := strings.Split(t.Format(time.StampMilli), ".")[1]
		id := row[0]
		idParsed, _ := strconv.ParseInt(id, 10, 64)
		if lastTradeID >= idParsed {
			continue
		}
		lastTradeID = idParsed
		price := row[1]
		qty := row[2]
		oper := "S"
		if strings.ToUpper(row[5]) == "TRUE" {
			oper = "B"
		}
		err = writer.Write([]string{symbol, date, timestamp, msec, id, price, qty, oper})
		if err != nil {
			log.Fatalf("Error writing to CSV: %v", err)
		}
	}
	return lastWriteData, lastTradeID, nil
}

func (t *Trades) GetFileName(dir string, symbol string, period string) string {
	return path.Join(dir, fmt.Sprintf("%s-%s.txt", symbol, TradesDataType))
}
