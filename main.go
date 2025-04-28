package main

import (
	"binanceklinedumper/dumper"
	"binanceklinedumper/util"
	"context"
	"flag"
	"github.com/pkg/errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

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
	flag.StringVar(&dataType, "type", "", "Supported: klines, trades. Check here: https://data.binance.vision/?prefix=data/futures/um/daily/")
	flag.StringVar(&dataDir, "data-dir", "data", "path to data directory")
	flag.Parse()

	util.InitSymbolData(symbolsStr)

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

	if err := os.MkdirAll(dataDir, os.ModePerm); err != nil {
		log.Fatal(errors.Wrapf(err, "failed to create data directory in path: %s", dataDir))
	}

	processSymbols(ctx, &wg, workers, start, end, dataDir, period, dataType)

	wg.Wait()
	log.Println("Shutting down gracefully...")
	cancel()
	log.Println("All workers done, exiting.")
}

func processSymbols(ctx context.Context, wg *sync.WaitGroup, workers chan struct{}, start string, end string, dataDir string, period string, dataType string) {
	startDate, err := time.Parse("2006-01-02", start)
	if err != nil {
		log.Fatalln("Error parsing start date:", err)
	}
	endDate, err := time.Parse("2006-01-02", end)
	if err != nil {
		log.Println("Error parsing end date. Using now time")
		endDate = time.Now().UTC()
	}

	for _, item := range util.Symbols {
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
			dataDumper := dumper.New(dataDir, symbol, dataType, period, startDate, endDate)
			if processErr := dataDumper.ProcessSymbol(ctx); processErr != nil {
				log.Println(processErr)
			}
		}(item)
	}
}
