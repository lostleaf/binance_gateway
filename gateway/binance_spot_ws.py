import logging

import pandas as pd

from .constant import CandleData

from .websocket_client import WebsocketClient


class BinanceSpotWs(WebsocketClient):
    """币安现货行情Websocket API"""
    def __init__(self) -> None:
        """构造函数"""
        super().__init__()
        self.reqid = 0

    def connect(self):
        """连接Websocket行情频道"""
        self.init("wss://stream.binance.com:9443/stream")

        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        print('行情Websocket API连接成功')
        logging.info("行情Websocket API连接成功")

        # 重新订阅行情
        req: dict = {"method": "SUBSCRIBE", "params": ['btcusdt@kline_1m'], "id": self.reqid}
        self.send_packet(req)
        self.reqid += 1

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        stream: str = packet.get("stream", None)

        if not stream:
            return

        if 'kline' in stream:
            d = packet['data']['k']
            if not d['x']: # candle not closed
                return
            candle = CandleData(candle_begin_time=pd.to_datetime(int(d['t']), unit='ms', utc=True),
                                caldne_end_time=pd.to_datetime(int(d['T']), unit='ms', utc=True),
                                open=float(d['o']),
                                high=float(d['h']),
                                low=float(d['l']),
                                close=float(d['c']),
                                volume=float(d['v']),
                                turnover=float(d['q']),
                                num_trades=int(d['n']),
                                buy_vol=float(d['V']),
                                buy_turnover=float(d['Q']))
            print(candle)
