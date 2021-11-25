import math
from datetime import datetime
from typing import Optional, Union

import ccxt
import pandas as pd

from .constant import (EXCHANGE_TIMEOUT_MS, AccountData, CandleData, Direction, OrderbookData, OrderData, OrderStatus,
                        OrderType, PositionData, SymbolData, SymbolType)
from .util import (floor_to_tick, get_timeframe_delta, retry_getter, round_to_tick)

SPOT_QUOTES = ['USDT', 'BUSD', 'TUSD', 'USDC', 'BKRW']

SymTypeOrList = Union[SymbolType, list[SymbolType]]

ORDERTYPE_CC2EXG: dict[OrderType, tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.IOC: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_EXG2CC: dict[tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_CC2EXG.items()}

DIRECTION_CC2EXG: dict[Direction, str] = {Direction.LONG: "BUY", Direction.SHORT: "SELL"}
DIRECTION_EXG2CC: dict[str, Direction] = {v: k for k, v in DIRECTION_CC2EXG.items()}

MAX_CANDLES: dict[SymbolType, int] = {
    SymbolType.SPOT: 1000,
    SymbolType.FUTURES_COIN: 1500,
    SymbolType.FUTURES_USDT: 1500,
    SymbolType.SWAP_COIN: 1500,
    SymbolType.SWAP_USDT: 1500
}

TRANSFER_WALLET_CC2EXG: dict[SymbolType, str] = {
    SymbolType.SPOT: 'MAIN',
    SymbolType.FUTURES_COIN: 'CMFUTURE',
    SymbolType.SWAP_COIN: 'CMFUTURE',
    SymbolType.FUTURES_USDT: 'UMFUTURE',
    SymbolType.SWAP_USDT: 'UMFUTURE',
}

STATUS_EXG2CC: dict[str, OrderStatus] = {
    "NEW": OrderStatus.OPEN,
    "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
    "FILLED": OrderStatus.FULLY_FILLED,
    "CANCELED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED,
    "EXPIRED": OrderStatus.CANCELED
}


class BinanceGateway:
    CLS_ID = 'BA'

    def __init__(self, apiKey=None, secret=None):
        self.exg = ccxt.binance({
            'apiKey': apiKey,
            'secret': secret,
            'timeout': EXCHANGE_TIMEOUT_MS,
        })
        self.sym_info = self.query_symbol([SymbolType.SWAP_COIN, SymbolType.SWAP_USDT, SymbolType.SPOT])

    @staticmethod
    def convert_symbol_exg_to_cc(exg_symbol: str, sym_type: SymbolType) -> str:
        return _convert_symbol_exg_to_cc(exg_symbol, sym_type)

    @staticmethod
    def convert_symbol_cc_to_exg(cc_symbol: str) -> tuple[str, SymbolType]:
        return _convert_symbol_cc_to_exg(cc_symbol)

    def query_account(self, sym_type: SymTypeOrList) -> dict[str, AccountData]:
        if isinstance(sym_type, SymbolType):
            sym_type = [sym_type]

        account = dict()

        if SymbolType.FUTURES_COIN in sym_type or SymbolType.SWAP_COIN in sym_type:
            data = retry_getter(self.exg.dapiPrivate_get_account)
            for x in data['assets']:
                acc_info = parse_account(x)
                account[f'{x["asset"]}.{SymbolType.FUTURES_COIN.value}'] = acc_info
                account[f'{x["asset"]}.{SymbolType.SWAP_COIN.value}'] = acc_info

        if SymbolType.FUTURES_USDT in sym_type or SymbolType.SWAP_USDT in sym_type:
            data = retry_getter(self.exg.fapiPrivate_get_account)
            for x in data['assets']:
                acc_info = parse_account(x)
                account[f'{x["asset"]}.{SymbolType.FUTURES_USDT.value}'] = acc_info
                account[f'{x["asset"]}.{SymbolType.SWAP_USDT.value}'] = acc_info

        if SymbolType.SPOT in sym_type:
            data = retry_getter(self.exg.private_get_account)
            for x in data['balances']:
                acc_info = parse_account(x)
                account[f'{x["asset"]}.{SymbolType.SPOT.value}'] = acc_info

        return account

    def query_position(self, sym_type: SymTypeOrList) -> dict[str, PositionData]:
        if isinstance(sym_type, SymbolType):
            sym_type = [sym_type]

        position = dict()

        if SymbolType.FUTURES_COIN in sym_type or SymbolType.SWAP_COIN in sym_type:
            data = retry_getter(self.exg.dapiPrivate_get_positionrisk)
            for x in data:
                cc_symbol = convert_coin_symbol_exg_to_cc(x['symbol'])
                position[cc_symbol] = parse_position(x, cc_symbol)

        if SymbolType.FUTURES_USDT in sym_type or SymbolType.SWAP_USDT in sym_type:
            data = retry_getter(self.exg.fapiPrivate_get_positionrisk)
            for x in data:
                cc_symbol = convert_usdt_symbol_exg_to_cc(x['symbol'])
                position[cc_symbol] = parse_position(x, cc_symbol)

        return position

    def query_account_and_position(self,
                                   sym_type: SymTypeOrList) -> tuple[dict[str, AccountData], dict[str, PositionData]]:
        if isinstance(sym_type, SymbolType):
            sym_type = [sym_type]

        account, position = dict(), dict()

        if SymbolType.FUTURES_COIN in sym_type or SymbolType.SWAP_COIN in sym_type:
            account.update(self.query_account([SymbolType.FUTURES_COIN, SymbolType.SWAP_COIN]))
            position.update(self.query_position([SymbolType.FUTURES_COIN, SymbolType.SWAP_COIN]))

        if SymbolType.FUTURES_USDT in sym_type or SymbolType.SWAP_USDT in sym_type:
            data = retry_getter(self.exg.fapiPrivate_get_account)
            for x in data['assets']:
                acc_info = parse_account(x)
                account[f'{x["asset"]}.{SymbolType.FUTURES_USDT.value}'] = acc_info
                account[f'{x["asset"]}.{SymbolType.SWAP_USDT.value}'] = acc_info
            for x in data['positions']:
                cc_symbol = convert_usdt_symbol_exg_to_cc(x['symbol'])
                position[cc_symbol] = parse_position(x, cc_symbol)

        if SymbolType.SPOT in sym_type:
            account.update(self.query_account([SymbolType.SPOT]))
            position.update(self.query_position([SymbolType.SPOT]))

        return account, position

    def query_symbol(self, sym_type: SymTypeOrList) -> dict[str, SymbolData]:
        if isinstance(sym_type, SymbolType):
            sym_type = [sym_type]

        symbol = dict()

        if SymbolType.FUTURES_COIN in sym_type or SymbolType.SWAP_COIN in sym_type:
            data = retry_getter(self.exg.dapiPublic_get_exchangeinfo)
            for x in data['symbols']:
                if x['contractType'] == 'PERPETUAL':
                    type_ = SymbolType.SWAP_COIN
                elif x['contractType'].endswith('QUARTER'):
                    type_ = SymbolType.FUTURES_COIN
                else:
                    continue
                cc_symbol = self.convert_symbol_exg_to_cc(x['symbol'], type_)
                symbol[cc_symbol] = parse_symbol(x, cc_symbol)

        if SymbolType.FUTURES_USDT in sym_type or SymbolType.SWAP_USDT in sym_type:
            data = retry_getter(self.exg.fapiPublic_get_exchangeinfo)
            for x in data['symbols']:
                if x['contractType'] == 'PERPETUAL':
                    type_ = SymbolType.SWAP_USDT
                elif x['contractType'].endswith('QUARTER'):
                    type_ = SymbolType.FUTURES_USDT
                else:
                    continue
                cc_symbol = self.convert_symbol_exg_to_cc(x['symbol'], type_)
                symbol[cc_symbol] = parse_symbol(x, cc_symbol)

        if SymbolType.SPOT in sym_type:
            data = retry_getter(self.exg.public_get_exchangeinfo)
            for x in data['symbols']:
                cc_symbol = self.convert_symbol_exg_to_cc(x['symbol'], SymbolType.SPOT)
                symbol[cc_symbol] = parse_symbol(x, cc_symbol)

        return symbol

    def query_order(self, cc_symbol: str, order_id: str, cliend_order_id: Optional[str] = None) -> OrderData:
        exg_sym, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)

        params = {'symbol': exg_sym, 'orderId': order_id}

        if sym_type == SymbolType.FUTURES_COIN or sym_type == SymbolType.SWAP_COIN:
            data = retry_getter(lambda: self.exg.dapiPrivate_get_order(params))

        if sym_type == SymbolType.FUTURES_USDT or sym_type == SymbolType.SWAP_USDT:
            data = retry_getter(lambda: self.exg.fapiPrivate_get_order(params))

        if sym_type == SymbolType.SPOT:
            if cliend_order_id is not None:
                params = {'symbol': exg_sym, 'origClientOrderId': cliend_order_id}
            data = retry_getter(lambda: self.exg.private_get_order(params))
        return parse_order(data, cc_symbol, 'query')

    def query_orderbook(self, cc_symbol: str, limit=50) -> OrderbookData:
        exg_sym, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)
        params = {'symbol': exg_sym, 'limit': limit}

        if sym_type == SymbolType.FUTURES_COIN or sym_type == SymbolType.SWAP_COIN:
            data = retry_getter(lambda: self.exg.dapiPublic_get_depth(params))

        if sym_type == SymbolType.FUTURES_USDT or sym_type == SymbolType.SWAP_USDT:
            data = retry_getter(lambda: self.exg.fapiPublic_get_depth(params))

        if sym_type == SymbolType.SPOT:
            data = retry_getter(lambda: self.exg.public_get_depth(params))

        ask_prices, ask_sizes = list(zip(*data['asks']))
        bid_prices, bid_sizes = list(zip(*data['bids']))

        ask_prices = [float(x) for x in ask_prices]
        bid_prices = [float(x) for x in bid_prices]
        ask_sizes = [float(x) for x in ask_sizes]
        bid_sizes = [float(x) for x in bid_sizes]

        return OrderbookData(ask_prices=ask_prices, ask_sizes=ask_sizes, bid_prices=bid_prices, bid_sizes=bid_sizes)

    def query_candle(self, cc_symbol: str, start: datetime, end: datetime, timeframe: str) -> list[CandleData]:
        exg_sym, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)
        max_candles = MAX_CANDLES[sym_type]

        timeframe_dlt = get_timeframe_delta(timeframe)
        cur_time = start
        results: list[CandleData] = []

        while cur_time < end:
            num_to_end = math.ceil((end - cur_time) / timeframe_dlt)
            limit = min(num_to_end, max_candles)
            params = {
                'symbol': exg_sym,
                'interval': timeframe,
                'startTime': int(cur_time.timestamp()) * 1000,
                'endTime': int((cur_time + (limit - 1) * timeframe_dlt).timestamp()) * 1000,
                'limit': limit
            }
            if sym_type == SymbolType.FUTURES_COIN or sym_type == SymbolType.SWAP_COIN:
                data = retry_getter(lambda: self.exg.dapiPublic_get_klines(params))

            if sym_type == SymbolType.FUTURES_USDT or sym_type == SymbolType.SWAP_USDT:
                data = retry_getter(lambda: self.exg.fapiPublic_get_klines(params))

            if sym_type == SymbolType.SPOT:
                data = retry_getter(lambda: self.exg.public_get_klines(params))

            if not data:
                break

            for d in data:
                results.append(
                    CandleData(candle_begin_time=pd.to_datetime(int(d[0]), unit='ms', utc=True),
                               caldne_end_time=pd.to_datetime(int(d[6]), unit='ms', utc=True),
                               open=float(d[1]),
                               high=float(d[2]),
                               low=float(d[3]),
                               close=float(d[4]),
                               volume=float(d[5]),
                               turnover=float(d[7]),
                               num_trades=int(d[8]),
                               buy_vol=float(d[9]),
                               buy_turnover=float(d[10])))

            cur_time = results[-1].candle_begin_time + timeframe_dlt

        return results

    def send_order(self,
                   cc_symbol: str,
                   direction: Direction,
                   order_type: OrderType,
                   price: float,
                   size: float,
                   reference: Optional[str] = None) -> OrderData:
        order_type, time_condition = ORDERTYPE_CC2EXG[order_type]
        exg_sym, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)
        sym_info = self.sym_info[cc_symbol]
        params = {
            "symbol": exg_sym,
            "side": DIRECTION_CC2EXG[direction],
            "type": order_type,
            "timeInForce": time_condition,
            "price": round_to_tick(price, sym_info.price_tick),
            "quantity": floor_to_tick(size, sym_info.size_tick),
        }
        if reference is not None:
            params['newClientOrderId'] = reference

        if sym_type == SymbolType.FUTURES_COIN or sym_type == SymbolType.SWAP_COIN:
            data = retry_getter(lambda: self.exg.dapiPrivate_post_order(params))

        if sym_type == SymbolType.FUTURES_USDT or sym_type == SymbolType.SWAP_USDT:
            data = retry_getter(lambda: self.exg.fapiPrivate_post_order(params))

        if sym_type == SymbolType.SPOT:
            data = retry_getter(lambda: self.exg.private_post_order(params))

        return parse_order(data, cc_symbol, 'send')

    def batch_send_orders(self, orders: dict[tuple[str, Direction], dict]) -> dict[str, OrderData]:
        coin_orders = []
        usdt_orders = []
        spot_orders = []
        for (cc_symbol, order_dir), order in orders.items():
            exg_sym, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)
            order_type, time_condition = ORDERTYPE_CC2EXG[order['order_type']]
            sym_info = self.sym_info[cc_symbol]
            order_params = {
                "symbol": exg_sym,
                "side": DIRECTION_CC2EXG[order_dir],
                "type": order_type,
                "timeInForce": time_condition,
                "price": str(round_to_tick(order['price'], sym_info.price_tick)),
                "quantity": str(floor_to_tick(order['size'], sym_info.size_tick)),
            }
            if sym_type == SymbolType.FUTURES_COIN or sym_type == SymbolType.SWAP_COIN:
                coin_orders.append(order_params)
            if sym_type == SymbolType.FUTURES_USDT or sym_type == SymbolType.SWAP_USDT:
                usdt_orders.append(order_params)
            if sym_type == SymbolType.SPOT:
                spot_orders.append(order_params)

        NUM = 5  # 批量下单的数量

        result = dict()
        coin_data = []
        for i in range(0, len(coin_orders), NUM):
            params = {'batchOrders': self.exg.json(coin_orders[i:i + NUM])}
            coin_data.extend(retry_getter(lambda: self.exg.dapiPrivatePostBatchOrders(params)))
        for x in coin_data:
            cc_symbol = convert_coin_symbol_exg_to_cc(x['symbol'])
            order = parse_order(x, cc_symbol, 'send')
            result[(cc_symbol, order.direction)] = order

        usdt_data = []
        for i in range(0, len(usdt_orders), NUM):
            params = {'batchOrders': self.exg.json(usdt_orders[i:i + NUM])}
            usdt_data.extend(retry_getter(lambda: self.exg.fapiPrivatePostBatchOrders(params)))
        for x in usdt_data:
            cc_symbol = convert_usdt_symbol_exg_to_cc(x['symbol'])
            order = parse_order(x, cc_symbol, 'send')
            result[(cc_symbol, order.direction)] = order

        for params in spot_orders:
            data = retry_getter(lambda: self.exg.private_post_order(params))
            cc_symbol = self.convert_symbol_exg_to_cc(data['symbol'], SymbolType.SPOT)
            order = parse_order(data, cc_symbol, 'send')
            result[(cc_symbol, order.direction)] = order
        return result

    def cancel_order(self, cc_symbol: str, order_id: str) -> OrderData:
        exg_sym, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)

        params = {'symbol': exg_sym, 'orderId': order_id}

        if sym_type == SymbolType.FUTURES_COIN or sym_type == SymbolType.SWAP_COIN:
            data = retry_getter(lambda: self.exg.dapiPrivate_delete_order(params))

        if sym_type == SymbolType.FUTURES_USDT or sym_type == SymbolType.SWAP_USDT:
            data = retry_getter(lambda: self.exg.fapiPrivate_delete_order(params))

        return parse_order(data, cc_symbol, 'cancel')

    def transfer_asset(self, from_wallet: SymbolType, to_wallet: SymbolType, currency: str, amount: float):
        transfer_type = f'{TRANSFER_WALLET_CC2EXG[from_wallet]}_{TRANSFER_WALLET_CC2EXG[to_wallet]}'
        params = {'type': transfer_type, 'asset': currency, 'amount': amount}
        retry_getter(lambda: self.exg.sapiPostAssetTransfer(params))

    def get_swap_funding_fee_rate_history(self, cc_symbol):
        exg_symbol, sym_type = self.convert_symbol_cc_to_exg(cc_symbol)
        if sym_type == SymbolType.SWAP_COIN:
            data = retry_getter(lambda: self.exg.dapiPublic_get_fundingrate({'symbol': exg_symbol}), raise_err=True)
        if sym_type == SymbolType.SWAP_USDT:
            data = retry_getter(lambda: self.exg.fapiPublic_get_fundingrate({'symbol': exg_symbol}), raise_err=True)
        data = [{
            'symbol': cc_symbol,
            'funding_time': pd.to_datetime(x['fundingTime'], unit='ms', utc=True),
            'rate': float(x['fundingRate'])
        } for x in data]
        return data

    def get_swap_recent_fee_rate(self):
        data = retry_getter(self.exg.dapiPublic_get_premiumindex, raise_err=True)
        drates = [{
            'symbol': self.convert_symbol_exg_to_cc(x['symbol'], SymbolType.SWAP_COIN),
            'funding_time': pd.to_datetime(x['nextFundingTime'], unit='ms', utc=True),
            'rate': float(x['lastFundingRate'])
        } for x in data if x['lastFundingRate'] != '']
        data = retry_getter(self.exg.fapiPublic_get_premiumindex, raise_err=True)
        frates = [{
            'symbol': self.convert_symbol_exg_to_cc(x['symbol'], SymbolType.SWAP_USDT),
            'funding_time': pd.to_datetime(x['nextFundingTime'], unit='ms', utc=True),
            'rate': float(x['lastFundingRate'])
        } for x in data if x['lastFundingRate'] != '']
        return drates + frates


def parse_account(x: dict) -> AccountData:
    if 'marginBalance' in x:
        equity = float(x['marginBalance'])  # FUTURES
        balance = float(x['walletBalance'])  # FUTURES
        unrealized_pnl = float(x['unrealizedProfit'])
    elif 'free' in x:
        balance = equity = float(x['free'])  # SPOT
        unrealized_pnl = 0.
    return AccountData(account_id=x['asset'], equity=equity, balance=balance, unrealized_pnl=unrealized_pnl)


def parse_position(x: dict, cc_symbol: str) -> PositionData:
    size = float(x['positionAmt'])
    direction = None if size == 0 else (Direction.LONG if size > 0 else Direction.SHORT)
    if 'unrealizedProfit' in x:
        unrealized_pnl = float(x['unrealizedProfit'])
    elif 'unRealizedProfit' in x:
        unrealized_pnl = float(x['unRealizedProfit'])
    return PositionData(cc_symbol=cc_symbol,
                        direction=direction,
                        size=size,
                        price=float(x['entryPrice']),
                        unrealized_pnl=unrealized_pnl)


def parse_symbol(x: dict, cc_symbol: str) -> SymbolData:
    price_tick = 1
    size_tick = 1
    face_value = float(x.get('contractSize', 1))

    for f in x["filters"]:
        if f["filterType"] == "PRICE_FILTER":
            price_tick = float(f["tickSize"])
        elif f["filterType"] == "LOT_SIZE":
            size_tick = float(f["stepSize"])

    return SymbolData(cc_symbol=cc_symbol, size_tick=size_tick, price_tick=price_tick, face_value=face_value)


def parse_order(x: dict, cc_symbol: str, type_: str) -> OrderData:
    key = (x["type"], x["timeInForce"])
    order_type = ORDERTYPE_EXG2CC.get(key, None)
    if type_ == 'send':
        if 'updateTime' in x:
            ts = pd.to_datetime(int(x['updateTime']), unit='ms', utc=True)
        elif 'transactTime' in x:
            ts = pd.to_datetime(int(x['transactTime']), unit='ms', utc=True)
    if type_ == 'query':
        ts = pd.to_datetime(int(x['time']), unit='ms', utc=True)
    if type_ == 'cancel':
        ts = pd.NaT
    return OrderData(cc_symbol=cc_symbol,
                     order_id=x['orderId'],
                     timestamp=ts,
                     type=order_type,
                     direction=DIRECTION_EXG2CC[x['side']],
                     status=STATUS_EXG2CC[x["status"]],
                     price=float(x['price']),
                     size=float(x['origQty']),
                     filled_price=float(x.get("avgPrice", 0)),
                     filled_size=float(x.get("executedQty", 0)),
                     cliend_order_id=x.get('clientOrderId', ''))


def _convert_symbol_exg_to_cc(exg_symbol: str, sym_type: SymbolType) -> str:
    """
        {base}-{quote}.{type}
        {base}-{quote}-{exp_date}.{type}
        """
    if sym_type == SymbolType.SPOT:  # BTCUSDT
        for quote in SPOT_QUOTES:
            if exg_symbol.endswith(quote):
                return f'{exg_symbol[:-len(quote)]}-{quote}.SPT'
        return f'{exg_symbol[:-3]}-{exg_symbol[-3:]}.SPT'

    if sym_type == SymbolType.FUTURES_USDT:  # BTCUSDT_210625
        underlying, exp_date = exg_symbol.split('_')
        return f'{underlying[:-4]}-USDT-{exp_date}.FUTU'

    if sym_type == SymbolType.FUTURES_COIN:  # BTCUSD_210625
        underlying, exp_date = exg_symbol.split('_')
        return f'{underlying[:-3]}-USD-{exp_date}.FUTC'

    if sym_type == SymbolType.SWAP_COIN:  # BTCUSD_PERP
        return f'{exg_symbol[:-8]}-USD.SWPC'

    if sym_type == SymbolType.SWAP_USDT:  # BTCUSDT
        return f'{exg_symbol[:-4]}-USDT.SWPU'


def _convert_symbol_cc_to_exg(cc_symbol: str) -> tuple[str, SymbolType]:
    symbol, sym_type = cc_symbol.split('.')
    sym_type = SymbolType(sym_type)

    if sym_type in (SymbolType.SPOT, SymbolType.SWAP_USDT):
        return symbol.replace('-', ''), sym_type

    if sym_type in (SymbolType.FUTURES_COIN, SymbolType.FUTURES_USDT):
        base, quote, exp_date = symbol.split('-')
        return f'{base}{quote}_{exp_date}', sym_type

    if sym_type == SymbolType.SWAP_COIN:
        return f'{symbol.replace("-", "")}_PERP', sym_type


def convert_coin_symbol_exg_to_cc(exg_symbol):
    if exg_symbol[-1].isdigit():  # futures symbol
        cc_symbol = _convert_symbol_exg_to_cc(exg_symbol, SymbolType.FUTURES_COIN)
    else:
        cc_symbol = _convert_symbol_exg_to_cc(exg_symbol, SymbolType.SWAP_COIN)
    return cc_symbol


def convert_usdt_symbol_exg_to_cc(exg_symbol):
    if exg_symbol[-1].isdigit():  # futures symbol
        cc_symbol = _convert_symbol_exg_to_cc(exg_symbol, SymbolType.FUTURES_USDT)
    else:
        cc_symbol = _convert_symbol_exg_to_cc(exg_symbol, SymbolType.SWAP_USDT)
    return cc_symbol
