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
	"time"

	"github.com/pkg/errors"
)

const MetricsDataType = "metrics"

type Metrics struct{}

func (m *Metrics) WriteHeader(file *os.File) error {
	_, err := fmt.Fprintln(file, "<TICKER>,<DATE>,<TIME>,<OI>")
	return err
}

func (m *Metrics) GetLastTimeWritten(row []string) (time.Time, int64, error) {
	if len(row) < 4 {
		return time.Time{}, 0, errors.New("invalid file format")
	}

	timeStr := row[1] + row[2]
	metricTime, err := time.Parse("20060102150405", timeStr)
	if err != nil {
		return time.Time{}, 0, errors.Wrap(err, "Error parsing last kline time")
	}

	return metricTime, 0, nil
}

func (m *Metrics) GetFileURL(symbol string, period string, timeRange string, dateStr string) (string, error) {
	fileURL := "https://data.binance.vision/data/futures/um"
	filePath := fmt.Sprintf("%s/metrics/%s/%s-metrics-%s.zip", timeRange, symbol, symbol, dateStr)

	var err error
	if fileURL, err = url.JoinPath(fileURL, filePath); err != nil {
		return "", errors.Wrapf(err, "failed to create the file URL from: %s and %s", fileURL, filePath)
	}
	return fileURL, nil
}

func (m *Metrics) Write(ctx context.Context, symbol string, _ string, csvReader *csv.Reader, writer *csv.Writer, lastDate time.Time, _ int64) (time.Time, int64, error) {
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, 0, nil
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
		openTimeMs, err := time.Parse("2006-01-02 15:04:05", row[0])
		if err != nil {
			log.Printf("Error parsing open_time: %v", err)
			continue
		}
		t := openTimeMs.UTC()
		if !lastDate.IsZero() && !t.After(lastDate) {
			continue
		}
		lastDate = t
		formattedDate := t.Format("20060102")
		formattedTime := t.Format("150405")
		oi := row[2]
		err = writer.Write([]string{symbol, formattedDate, formattedTime, oi})
		if err != nil {
			log.Printf("Error writing to CSV: %v", err)
		}
	}
	return lastDate, 0, nil
}

func (m *Metrics) GetFileName(dir string, symbol string, period string) string {
	return path.Join(dir, fmt.Sprintf("%s-%s.txt", symbol, MetricsDataType))
}
