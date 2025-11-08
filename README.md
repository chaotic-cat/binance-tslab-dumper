# binance-tslab-dumper
This is a dumper of a Binance perpetual futures historical data designed to create files for TSLab

At present, this dumper works for the following types of Binance data:
 - Klines:
   - Spot
     - Daily: https://data.binance.vision/?prefix=data/futures/daily/klines
   - Futures:
     - Daily: https://data.binance.vision/?prefix=data/futures/um/daily/klines
 - Trades: 
   - Spot
     - Daily: https://data.binance.vision/?prefix=data/futures/daily/trades
   - Futures
     - Daily: https://data.binance.vision/?prefix=data/futures/um/daily/trades 
 - Metrics: 
   - Futures:
     - Daily: https://data.binance.vision/?prefix=data/futures/um/daily/metrics

TSLab text file format description: https://doc.tslab.pro/tslab/eng/data-providers/historical-data/text-files-with-historical-data

## How to use
### Trades futures
`binance-tslab-dumper --symbols=ADAUSDT,XRPUSDT --futures=true --start=2025-11-01 --parallel=2 --type=trades`
### Trades spot
`binance-tslab-dumper --symbols=ADAUSDT,XRPUSDT --futures=false --start=2025-11-01 --parallel=2 --type=trades`
### Klines Futures
`binance-tslab-dumper --symbols=ADAUSDT,XRPUSDT --futures=true --start=2025-11-01 --parallel=2 --type=klines --period=1m`
### Klines spot
`binance-tslab-dumper --symbols=ADAUSDT,XRPUSDT --futures=false --start=2025-11-01 --parallel=2 --type=klines --period=1m`
### Metrics
`binance-tslab-dumper --symbols=ADAUSDT,XRPUSDT --start=2025-01-01 --parallel=2 --type=metrics`

If no symbols are specified - it will download data for all tradable futures with USDT suffix:
`binance-tslab-dumper --start=2025-01-01 --parallel=2 --type=metrics`

If specified data is less than first date in the file for a symbol - it will delete the file and create file with earlier data. 

It will download data and create a file in a trades, klines or metrics directory. After that you can specify as an offline TEXT data source in the TSLab.
