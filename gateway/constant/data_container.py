from dataclasses import dataclass
from .type import Direction, OrderType, OrderStatus
from datetime import datetime


@dataclass
class AccountData:
    """
    Account data contains information about equity and balance
    """

    account_id: str
    equity: float = 0  # equity = balance + unrealized_pnl
    balance: float = 0
    unrealized_pnl: float = 0


@dataclass
class PositionData:
    """
    Positon data is used for tracking each individual position holding
    """

    cc_symbol: str
    direction: Direction

    size: float = 0
    price: float = 0
    unrealized_pnl: float = 0


@dataclass
class SymbolData:
    """
    Symbol data contains basic information about each symbol
    """

    cc_symbol: str
    size_tick: float
    price_tick: float
    face_value: float


@dataclass
class OrderData:
    """
    Order data contains information for tracking lastest status of a specific order.
    """

    cc_symbol: str
    order_id: str
    timestamp: datetime

    type: OrderType
    direction: Direction
    status: OrderStatus
    price: float
    size: float

    filled_price: float = 0
    filled_size: float = 0

    cliend_order_id: str = ""


@dataclass
class OrderbookData:
    """
    Orderbook data
    """
    ask_prices: list[float]
    ask_sizes: list[float]
    bid_prices: list[float]
    bid_sizes: list[float]


@dataclass
class CandleData:
    """
    Orderbook data
    """
    candle_begin_time: datetime
    caldne_end_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    num_trades: int
    buy_vol: float
    buy_turnover: float
