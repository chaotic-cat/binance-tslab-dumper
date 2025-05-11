package util

import (
	"binance-tslab-dumper/domain"
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"strings"
)

var Symbols []string
var SymbolInfo map[string]domain.Info

func InitSymbolData(symbolsStr string) {
	if len(symbolsStr) == 0 {
		log.Println("No symbols. Requesting all active USDT futures from binance")
		infos := getSymbolsInfo()
		SymbolInfo = make(map[string]domain.Info, len(infos))
		for _, item := range infos {
			if item.Status != "TRADING" || !strings.HasSuffix(item.Symbol, "USDT") ||
				strings.Contains(item.Symbol, "_") {
				continue
			}
			Symbols = append(Symbols, item.Symbol)
			SymbolInfo[item.Symbol] = item
		}
	} else {
		Symbols = strings.Split(symbolsStr, ",")
		SymbolInfo = make(map[string]domain.Info, len(Symbols))
		infos := getSymbolsInfo()
		for _, item := range infos {
			SymbolInfo[item.Symbol] = item
		}
	}
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
