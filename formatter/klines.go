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

const KlinesDataType = "klines"

type Klines struct{}

func (k *Klines) WriteHeader(file *os.File) error {
	_, err := fmt.Fprintln(file, "<TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>")
	return err
}

func (k *Klines) GetLastTimeWritten(row []string) (time.Time, int64, error) {
	if len(row) < 9 {
		return time.Time{}, -1, errors.New("invalid file format")
	}

	timeStr := row[2] + row[3]
	klineTime, err := time.Parse("20060102150405", timeStr)
	if err != nil {
		return time.Time{}, -1, errors.Wrap(err, "Error parsing last kline time")
	}

	return klineTime, -1, nil
}

func (k *Klines) GetFileURL(symbol string, period string, timeRange string, dateStr string) (string, error) {
	fileURL := "https://data.binance.vision/data/futures/um"
	filePath := fmt.Sprintf("%s/klines/%s/%s/%s-%s-%s.zip", timeRange, symbol, period, symbol, period, dateStr)

	var err error
	if fileURL, err = url.JoinPath(fileURL, filePath); err != nil {
		return "", errors.Wrapf(err, "failed to create the file URL from: %s and %s", fileURL, filePath)
	}
	return fileURL, nil
}

func (k *Klines) Write(ctx context.Context, symbol string, period string, csvReader *csv.Reader, writer *csv.Writer, lastDate time.Time, _ int64) error {
	for {
		select {
		case <-ctx.Done():
			return nil
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
	return nil
}

func (k *Klines) GetFileName(dir string, symbol string, period string) string {
	dirName := strings.Join([]string{KlinesDataType, period}, "_")
	return path.Join(dir, fmt.Sprintf("%s-%s.txt", symbol, dirName))

}
