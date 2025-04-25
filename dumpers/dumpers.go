package dumpers

import (
	"archive/zip"
	"binanceklinedumper/formatters"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"time"
)

func DumpData(ctx context.Context, symbol string, period string, dataType string, writer *csv.Writer, lastDate time.Time, dateStr string, periodStr string) (time.Time, error) {
	log.Println("Fetching", dataType, periodStr, "data for:", symbol, dateStr)

	var url string
	switch dataType {
	case "klines":
		url = fmt.Sprintf("https://data.binance.vision/data/futures/um/%s/klines/%s/%s/%s-%s-%s.zip", periodStr, symbol, period, symbol, period, dateStr)
	case "trades":
		// https://data.binance.vision/data/futures/um/monthly/trades/BTCUSDT/BTCUSDT-trades-2025-03.zip
		// https://data.binance.vision/data/futures/um/daily/trades/1000000MOGUSDT/1000000MOGUSDT-trades-2025-04-23.zip
		url = fmt.Sprintf("https://data.binance.vision/data/futures/um/%s/trades/%s/%s-trades-%s.zip", periodStr, symbol, symbol, dateStr)
	}

	csvFile, err := httpZipToCsvReader(ctx, url, periodStr, symbol)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "failed to read csv data for %s %s", symbol, periodStr)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	// Skip header
	_, err = csvReader.Read()
	if err != nil {
		csvFile.Close()
		return time.Time{}, errors.Wrapf(err, "Error reading CSV header")
	}

	switch dataType {
	case "klines":
		if lastDate, err = formatters.Klines(ctx, csvReader, symbol, period, writer, lastDate); err != nil {
			return time.Time{}, errors.Wrapf(err, "failed to fetch klines for %s %s", symbol, periodStr)
		}
	case "trades":
		if lastDate, err = formatters.Trades(ctx, csvReader, writer, lastDate); err != nil {
			return time.Time{}, errors.Wrapf(err, "failed to fetch data for %s %s", symbol, periodStr)
		}
	}

	return lastDate, nil
}

func httpZipToCsvReader(ctx context.Context, url string, periodStr string, symbol string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid url %s", url)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "Error downloading %s", url)
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 404 {
			return nil, errors.Errorf("No %s data for %s", periodStr, symbol)
		}
		return nil, errors.Errorf("Non-200 status for %s: %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "Error reading response body")
	}
	reader := bytes.NewReader(body)
	zipReader, err := zip.NewReader(reader, int64(len(body)))
	if err != nil {
		return nil, errors.Wrapf(err, "Error opening ZIP")
	}
	if len(zipReader.File) == 0 {
		return nil, errors.Wrapf(err, "No files in ZIP for %s", url)
	}
	f := zipReader.File[0]
	csvFile, err := f.Open()
	if err != nil {
		return nil, errors.Wrapf(err, "Error opening CSV file")
	}

	return csvFile, nil
}
