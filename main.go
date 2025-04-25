package main

import (
	"binanceklinedumper/domain"
	"binanceklinedumper/dumpers"
	"binanceklinedumper/formatters"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"
)

var symbols []string
var symbolInfo map[string]domain.Info

func main() {
	var symbolsStr string
	var period string
	var start string
	var end string
	var parallel int
	var dataType string
	var dataDir string

	flag.StringVar(&symbolsStr, "symbols", "BTCUSDT,ETHUSDT,SOLUSDT", "Comma-separated list of symbols")
	flag.StringVar(&period, "period", "1m", "Kline period (e.g., 1m, 5m)")
	flag.StringVar(&start, "start", "2024-01-01", "Start date (YYYY-MM-DD)")
	flag.StringVar(&end, "end", "", "End date (YYYY-MM-DD)")
	flag.IntVar(&parallel, "parallel", 1, "Number of parallel processes")
	flag.StringVar(&dataType, "type", "", "Supported: klines, trades. Check here: https://data.binance.vision/?prefix=data/futures/um/monthly/")
	flag.StringVar(&dataDir, "data-dir", "data", "path to data directory")
	flag.Parse()

	symbols, symbolInfo = fetchSymbolData(symbolsStr)

	workers := make(chan struct{}, parallel)

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	go func() {
		<-sigs
		log.Println("Termination signal received")
		cancel()
	}()

	dataDir = path.Join(dataDir, dataType)
	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		log.Fatal(errors.Wrap(err, "failed to create data directory next to this binary file"))
	}

	processSymbols(ctx, symbols, &wg, workers, start, end, dataDir, period, dataType)

	wg.Wait()
	log.Println("Shutting down gracefully...")
	cancel()
	log.Println("All workers done, exiting.")
}

func processSymbols(ctx context.Context, symbols []string, wg *sync.WaitGroup, workers chan struct{}, start string, end string, dataDir string, period string, dataType string) {
	startDate, err := time.Parse("2006-01-02", start)
	if err != nil {
		log.Fatalln("Error parsing start date:", err)
	}
	endDate, err := time.Parse("2006-01-02", end)
	if err != nil {
		log.Println("Error parsing end date. Using now time")
		endDate = time.Now().UTC()
	}

	for _, item := range symbols {
		select {
		case <-ctx.Done():
			return
		default:
		}
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			default:
			}

			workers <- struct{}{}
			defer func() { <-workers }()
			if processErr := processSymbol(ctx, symbol, startDate, endDate, dataDir, period, dataType); processErr != nil {
				log.Println(processErr)
			}
		}(item)
	}
}

func processSymbol(ctx context.Context, symbol string, startDate time.Time, endDate time.Time, dataDir string, period string, dataType string) error {
	if startDate.UnixMilli() < symbolInfo[symbol].OnboardDate {
		startDate = time.UnixMilli(symbolInfo[symbol].OnboardDate).UTC()
	}

	var lastDate time.Time
	outputFilePath := path.Join(dataDir, fmt.Sprintf("%s-%s-%s.txt", symbol, period, dataType))
	var file *os.File
	if _, err := os.Stat(outputFilePath); os.IsNotExist(err) {
		file, err = os.Create(outputFilePath)
		if err != nil {
			return errors.Wrapf(err, "Error creating file for %s: %v", symbol, outputFilePath)
		}
		formatters.WriteHeader(file, dataType)
	} else {
		file, lastDate, err = formatters.GetLastDate(symbol, file, outputFilePath, startDate, dataType)
		if err != nil {
			if file != nil {
				file.Close()
			}
			log.Println("File it in wrong format. recreating it: " + outputFilePath)
			file, err = os.Create(outputFilePath)
			if err != nil {
				return errors.Wrapf(err, "Error creating file for %s: %v", symbol, outputFilePath)
			}
			formatters.WriteHeader(file, dataType)
		}
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	current := startDate
	if !lastDate.IsZero() {
		current = lastDate
	}
	for ; !current.After(endDate); current = current.AddDate(0, 1, 0) {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		dateStr := current.Format("2006-01")
		periodStr := "monthly"
		var err error
		if lastDate, err = dumpers.DumpData(ctx, symbol, period, dataType, writer, lastDate, dateStr, periodStr); err != nil {
			log.Printf("Error fetching monthly data for %s[%v]: %v", symbol, dateStr, err)
			for ; !current.After(endDate); current = current.AddDate(0, 0, 1) {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				dateStr = current.Format("2006-01-02")
				periodStr = "daily"
				if lastDate, err = dumpers.DumpData(ctx, symbol, period, dataType, writer, lastDate, dateStr, periodStr); err != nil {
					log.Printf("Error fetching daily data for %s[%v]: %v", symbol, dateStr, err)
				}
			}
		}
	}
	return nil
}
