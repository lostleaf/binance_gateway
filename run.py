from gateway.binance import BinanceGateway
from gateway.binance_spot_ws import BinanceSpotWs
import pandas as pd
from pprint import pp
import time

# gateway = BinanceGateway()
# data = gateway.query_candle('BTC-USD.SWPC', pd.to_datetime('20211124 00:00:00', utc=True),
#                             pd.to_datetime('20211125 00:00:00', utc=True), '1h')
# pp(data)

spot = BinanceSpotWs()
spot.connect()
time.sleep(60)
spot.stop()