package dumper

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

func (d *Dumper) DumpData(ctx context.Context, lastDate time.Time, lastTradeID int64) error {
	dateStr := lastDate.Format("2006-01-02")
	timeRange := "daily"
	log.Println("Fetching", d.dataType, "data for:", d.symbol, dateStr)

	fileURL, err := d.formatter.GetFileURL(d.symbol, d.period, timeRange, dateStr)
	if err != nil {
		return err
	}

	csvFile, err := httpZipToCsvFile(ctx, fileURL, timeRange, d.symbol)
	if err != nil {
		return errors.Wrapf(err, "failed to read csv data for %s %s", d.symbol, timeRange)
	}
	defer csvFile.Close()

	if err = d.dumpFile(ctx, lastDate, lastTradeID, csvFile); err != nil {
		return err
	}

	return nil
}

func (d *Dumper) dumpFile(ctx context.Context, lastDate time.Time, lastTradeID int64, csvFile io.ReadCloser) error {
	csvReader := csv.NewReader(csvFile)
	// Skip header
	_, err := csvReader.Read()
	if err != nil {
		csvFile.Close()
		return errors.Wrapf(err, "Error skipping CSV header")
	}
	file, err := os.OpenFile(d.fileName, os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		if os.IsNotExist(err) {
			// not exists. create new
			if file, err = os.OpenFile(d.fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, os.ModePerm); err != nil {
				return errors.Wrapf(err, "Error creating file %s", d.fileName)
			}
			// write header
			if err = d.formatter.WriteHeader(file); err != nil {
				log.Fatalln(errors.Wrapf(err, "failed to write header to file %s", file.Name()))
			}
		} else {
			return errors.Wrapf(err, "failed to open file %s", d.fileName)
		}
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer func() {
		writer.Flush()
	}()

	if err = d.formatter.Write(ctx, d.symbol, d.period, csvReader, writer, lastDate, lastTradeID); err != nil {
		return errors.Wrapf(err, "failed to save %s for %s %s", d.dataType, d.symbol, d.period)
	}

	return nil
}

func httpZipToCsvFile(ctx context.Context, url string, periodStr string, symbol string) (io.ReadCloser, error) {
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
