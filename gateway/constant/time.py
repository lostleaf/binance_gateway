import pytz

EXCHANGE_TIMEOUT_MS = 3000  #3s

SHORT_SLEEP_TIME_SEC = 1  # 用于和交易所交互时比较紧急的时间sleep，例如获取数据、下单
MEDIUM_SLEEP_TIME_SEC = 2  # 用于和交易所交互时不是很紧急的时间sleep，例如获取持仓
LONG_SLEEP_TIME_SEC = 10  # 用于较长的时间sleep

TIMEZONE_HKT = pytz.timezone('hongkong')
