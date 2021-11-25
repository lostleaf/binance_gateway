import logging
import time
from datetime import timedelta
from decimal import Decimal

import pandas as pd
import math


def retry_getter(func, retry_times=5, sleep_seconds=1, default=None, raise_err=True):
    for i in range(retry_times):
        try:
            return func()
        except Exception as e:
            logging.warning(f'An error occurred {str(e)}')
            if i == retry_times - 1 and raise_err:
                raise e
            time.sleep(sleep_seconds)
            sleep_seconds *= 2
    return default


def get_timeframe_delta(timeframe: str) -> timedelta:
    qty = int(timeframe[:-1])
    if timeframe[-1] == 'm':
        return timedelta(minutes=qty)
    elif timeframe[-1] == 'h':
        return timedelta(hours=qty)
    elif timeframe[-1] == 'd':
        return timedelta(days=qty)
    elif timeframe[-1] == 's':
        return timedelta(seconds=qty)
    else:
        raise RuntimeError(f'Unknown timeframe {timeframe}')


def round_to_tick(value: float, tick: float) -> float:
    """
    Round price/size to tick value.
    """
    value = Decimal(str(value))
    target = Decimal(str(tick))
    rounded = float(int(round(value / target)) * target)
    return rounded


def floor_to_tick(value: float, tick: float) -> float:
    """
    Round price/size to tick value.
    """
    value = Decimal(str(value))
    target = Decimal(str(tick))
    rounded = float(int(math.floor(value / target)) * target)
    return rounded