from enum import Enum


class Direction(Enum):
    LONG = 'long'
    SHORT = 'short'
    CLOSE_LONG = 'close_long'
    CLOSE_SHORT = 'close_short'


class OrderType(Enum):
    LIMIT = 'limit'
    MAKER_ONLY = 'maker_only'
    FOK = 'fok'
    IOC = 'ioc'


class OrderStatus(Enum):
    FAILED = 'failed'
    CANCELED = 'canceled'
    OPEN = 'open'
    PARTIALLY_FILLED = 'partially_filled'
    FULLY_FILLED = 'fully_filled'
    SUBMITTING = 'submitting'
    CANCELING = 'canceling'
    REJECTED = 'rejected'
    INCOMPLETE = 'incomplete'  # okex open + partially filled
    COMPLETE = 'complete'  # okex canceled + fully filled


class SymbolType(Enum):
    SPOT = 'SPT'
    FUTURES_COIN = 'FUTC'
    FUTURES_USDT = 'FUTU'
    SWAP_COIN = 'SWPC'
    SWAP_USDT = 'SWPU'
    OPTIONS_COIN = 'OPSC'
    OPTIONS_USDT = 'OPSU'

class Exchange(Enum):
    OKEX = 'OK'
    BINANCE = 'BA'
    HUOBI = 'HB'
    FTX = 'FTX'