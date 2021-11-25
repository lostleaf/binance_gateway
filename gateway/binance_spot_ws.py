from .websocket_client import WebsocketClient
import logging

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
        req: dict = {
            "method": "SUBSCRIBE",
            "params": ['btcusdt@kline_1m'],
            "id": self.reqid
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        stream: str = packet.get("stream", None)

        if not stream:
            return
        print(packet)
