# binance-tslab-dumper
This is a dumper of a Binance perpetual futures historical data designed to create files for TSLab

At present, this dumper works for the following types of Binance data:
 - Klines: 
   - Daily: https://data.binance.vision/?prefix=data/futures/um/daily/klines 
   - Monthly https://data.binance.vision/?prefix=data/futures/um/monthly/klines
 - Trades: 
   - Daily: https://data.binance.vision/?prefix=data/futures/um/daily/trades 
   - Monthly: https://data.binance.vision/?prefix=data/futures/um/monthly/trades
 - Metrics(OI only): 
   - Daily only: https://data.binance.vision/?prefix=data/futures/um/daily/metrics

TSLab text file format description: https://doc.tslab.pro/tslab/eng/data-providers/historical-data/text-files-with-historical-data

## How to use
### Trades
`binance-tslab-dumper --symbols=1000WHYUSDT,XRPUSDT --start=2025-01-01 --parallel=2 --type=trades`
### Klines
`binance-tslab-dumper --symbols=1000WHYUSDT,XRPUSDT --start=2025-01-01 --parallel=2 --type=klines`
### Metrics
`binance-tslab-dumper --symbols=1000WHYUSDT,XRPUSDT --start=2025-01-01 --parallel=2 --type=metrics`

If no symbols are specified - it will download data for all tradable futures with USDT suffix:
`binance-tslab-dumper --start=2025-01-01 --parallel=2 --type=metrics`

If specified data is less than first date in the file for a symbol - it will delete the file and create file with earlier data. 

It will download data and create a file in a trades, klines or metrics directory. After that you can specify as an offline TEXT data source in the TSLab.
