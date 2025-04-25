package main

import (
	"binanceklinedumper/domain"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"strings"
)

func fetchSymbolData(symbolsStr string) ([]string, map[string]domain.Info) {
	symbols = strings.Split(symbolsStr, ",")
	symbolInfo = make(map[string]domain.Info, len(symbols))
	if len(symbols) == 0 {
		log.Println("No symbols. Requesting all USDT futures from binance")
		infos := getSymbolsInfo()
		symbolInfo = make(map[string]domain.Info, len(infos))
		for _, item := range infos {
			if item.Status != "TRADING" || !strings.HasSuffix(item.Symbol, "USDT") ||
				strings.Contains(item.Symbol, "_") {
				continue
			}
			symbols = append(symbols, item.Symbol)
			symbolInfo[item.Symbol] = item
		}
	} else {
		infos := getSymbolsInfo()
		for _, item := range infos {
			symbolInfo[item.Symbol] = item
		}
	}
	return symbols, symbolInfo
}

func getSymbolsInfo() []domain.Info {
	resp, err := http.Get("https://fapi.binance.com/fapi/v1/exchangeInfo")
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to list futures from binance"))
	}
	var infos domain.ExchangeInfoResponse
	if err = json.NewDecoder(resp.Body).Decode(&infos); err != nil {
		body, _ := io.ReadAll(resp.Body)
		log.Fatal(errors.Wrapf(err, "failed to parse futures from binance: %v", string(body)))
	}
	resp.Body.Close()
	return infos.Symbols
}
