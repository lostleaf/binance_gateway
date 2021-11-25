from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, Union

from .constant import (AccountData, CandleData, Direction, OrderbookData, OrderData, OrderType, PositionData, SymbolData,
                      SymbolType)

SymTypeOrList = Union[SymbolType, list[SymbolType]]


class BaseGateway(ABC):
    CLS_ID = 'base'

    @staticmethod
    @abstractmethod
    def convert_symbol_exg_to_cc(exg_symbol: str, sym_type: SymbolType) -> str:
        """
        Convert pair of exchange symbol and symbol type to cc_symbol
        {base}-{quote}|{type}|{exchange}
        {base}-{quote}-{exp_date}|{type}|{exchange}
        """
        pass

    @staticmethod
    @abstractmethod
    def convert_symbol_cc_to_exg(cc_symbol: str) -> tuple[str, SymbolType]:
        """
        Convert cc_symbol to pair of exchange symbol and symbol type
        """
        pass

    @abstractmethod
    def query_account(self, sym_type: SymTypeOrList) -> dict[str, AccountData]:
        """
        Query all account equities of given type(s)
        Return a mapping from account id to AccountData
        """
        pass

    @abstractmethod
    def query_position(self, sym_type: SymTypeOrList) -> dict[str, PositionData]:
        """
        Query all account positions of given type(s)
        Return a mapping from cc_symbol to PositionData
        """
        pass

    @abstractmethod
    def query_account_and_position(self,
                                   sym_type: SymTypeOrList) -> tuple[dict[str, AccountData], dict[str, PositionData]]:
        pass

    @abstractmethod
    def query_symbol(self, sym_type: SymTypeOrList) -> dict[str, SymbolData]:
        """
        Query all symbol basic info of given type(s)
        Return a mapping from cc_symbol to SymbolData
        """
        pass

    @abstractmethod
    def query_order(self, cc_symbol: str, order_id: str) -> OrderData:
        """
        Query the lastest status of a given order
        """
        pass

    @abstractmethod
    def query_orderbook(self, cc_symbol: str) -> OrderbookData:
        """
        Query the orderbook status of the given symbol
        """
        pass

    @abstractmethod
    def query_candle(self, cc_symbol: str, start: datetime, end: datetime, timeframe: str) -> list[CandleData]:
        """
        Query candlestick data with start <= candle_begin_time < end
        """
        pass

    @abstractmethod
    def send_order(self,
                   cc_symbol: str,
                   direction: Direction,
                   order_type: OrderType,
                   price: float,
                   size: float,
                   reference: Optional[str] = None) -> OrderData:
        """
        Send order
        """
        pass

    @abstractmethod
    def batch_send_orders(self, orders: dict[tuple[str, Direction], dict]) -> dict[str, OrderData]:
        """
        Batch send multiple orders
        """
        pass

    @abstractmethod
    def cancel_order(self, cc_symbol: str, order_id: str) -> OrderData:
        """
        Cancel order
        """
        pass

    @abstractmethod
    def transfer_asset(self, from_wallet: SymbolType, to_wallet: SymbolType, currency: str, amount: float):
        """
        Transfer between wallets
        """
        pass
