import datetime
import json
import operator
from datetime import date, datetime, timedelta
from dataclasses import dataclass

from dataclasses_json import dataclass_json, Undefined, CatchAll
from typing import Optional
import logging
import sys
import urllib.request
from typing import List

from marshmallow import EXCLUDE

from postgresql import *
from enum import Enum, auto
from pandas._libs.tslibs.timestamps import Timestamp
from itertools import groupby
from operator import itemgetter
from multiprocessing import Pool
import pandas as pd
import inspect
import psycopg2
from configparser import ConfigParser
from dataclasses import dataclass

logFormat = '%(asctime)s - %(levelname)s - %(threadName)s - [%(message)s]'
logFormatter = logging.Formatter(logFormat)
# logging.basicConfig(filename='test.log', format=logFormat, level=logging.DEBUG)
logging.basicConfig(filename='test.log', format=logFormat, level=logging.INFO)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger = logging.getLogger()
rootLogger.addHandler(consoleHandler)


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class CandleStickRaw:
    id: int
    high: float
    low: float
    close: float
    open: float
    time: str
    interval_min: int
    code: str


@dataclass_json
@dataclass
class GannSquareResult:
    base: int
    upTrendLevel: List[float]
    downTrendLevel: List[float]


@dataclass
class CandleStick(CandleStickRaw):
    timestamp: Timestamp
    time_group: str
    week_group: str
    month_group: str


class RESULT(Enum):
    PROFIT = auto()
    LOSS = auto()


class GANN_TARGET(Enum):
    SECOND = auto()
    THIRD = auto()


class DIRECTION(Enum):
    UP = auto()
    DOWN = auto()



@dataclass
class GannResult:
    base: float = None
    result: RESULT = None
    gann_sauare_target: GANN_TARGET = None
    profit_and_loss: float = None
    direction: DIRECTION = None
    executed: bool = False
    executed_price: float = None
    stop_loss_price: float = None


def market_data_of(code, interval, start, end):
    url_link = f"http://fishfish.sytes.net:13000/market_price_asc_view?and=(code.eq.{code},interval_min.eq.{interval},time.gte.{start},time.lt.{end})"
    info(url_link)
    with urllib.request.urlopen(url_link) as url:
        list_of_raw_json = json.loads(url.read())
        # info(list_of_raw_json)
        return [CandleStickRaw.from_json(json.dumps(raw_json)) for raw_json in list_of_raw_json]


def gannn_square_result_of(base: float, digit: int, decimal_point: int) -> GannSquareResult:
    target_base: float = round(base, decimal_point)
    url_link: str = f"https://gannsquare.herokuapp.com/gannaSqure/of/{target_base}/with/{digit}/sensitivity"
    info(url_link)
    with urllib.request.urlopen(url_link) as url:
        return GannSquareResult.from_json(json.dumps(json.loads(url.read())))


def debug(data):
    logging.debug(data)


def info(data):
    logging.info(data)


def warn(data):
    logging.warn(data)


def enrich(rawJson: CandleStickRaw) -> CandleStick:
    date_time_str = rawJson.time
    date_time = datetime.fromisoformat(date_time_str)
    timestamp: Timestamp = pd.Timestamp(date_time)
    return CandleStick(
        id=rawJson.id,
        high=rawJson.high,
        low=rawJson.low,
        close=rawJson.close,
        open=rawJson.open,
        time=rawJson.time,
        interval_min=rawJson.interval_min,
        code=rawJson.close,
        timestamp=timestamp,
        time_group=timestamp.ceil('-1D'),
        week_group=date_time.strftime("%Y-%U"),
        month_group=date_time.strftime("%Y-%m")
    )


def enrichMany(rawJsons: [CandleStick]):
    return [enrich(jsonDAta) for jsonDAta in rawJsons]


def find_next_up_gann_square(candle_stick_list: [CandleStick], date_range: int, digit: int, decimal_point: int) -> GannResult:
    candle_stick: CandleStick = candle_stick_list[2]
    time_limit: Timestamp = candle_stick.time_group + timedelta(date_range)
    gann_square_result: GannSquareResult = gannn_square_result_of(candle_stick.low, digit, decimal_point)
    info(f"checking base {candle_stick.low} for up trend with current time {candle_stick.timestamp} with time limit {time_limit}, up thred {gann_square_result.upTrendLevel}")
    result: GannResult = GannResult(direction=DIRECTION.Up, base=gann_square_result.base)
    if len(gann_square_result.upTrendLevel) == 3:
        first: float = gann_square_result.upTrendLevel[0]
        second: float = gann_square_result.upTrendLevel[1]
        third: float = gann_square_result.upTrendLevel[2]
        future_candle_stick_list: [CandleStick] = [x for x in candle_stick_list if x.time > candle_stick.time and x.time_group < time_limit]
        pass_first_level: [CandleStick] = [x for x in future_candle_stick_list if x.high > first]
        if len(pass_first_level) > 0:
            first_candle_stick_pass_first_level: CandleStick = pass_first_level[0]
            pass_first_level_list: [CandleStick] = [x for x in future_candle_stick_list if
                                                    x.time > first_candle_stick_pass_first_level.time]
            if len(pass_first_level_list) > 0:
                pass_second_level: [CandleStick] = [x for x in pass_first_level_list if x.high > second]
                if len(pass_second_level) > 0:
                    first_candle_stick_pass_second_level: CandleStick = pass_second_level[0]
                    pass_second_level_list: [CandleStick] = [x for x in pass_first_level_list if
                                                            x.time > first_candle_stick_pass_second_level.time]
                else:
                    info("second level fail and lost money")
            else:
                info("first level fail for up trend")
        else:
            info("first level fail for up trend")
    warn("error on qann square result")
    return result








if __name__ == '__main__':
    code: str = 'CADUSD'
    interval: int = 1
    date_rage: int = 4
    rawJson: [] = []
    today: datetime = date.today() - timedelta(days=1)
    for day in range(date_rage):
        to_date: datetime = today - timedelta(days=day)
        from_date: datetime = to_date - timedelta(days=1)
        info(f"{from_date.strftime('%Y-%m-%d')} to {to_date.strftime('%Y-%m-%d')}")
        rawJson += enrichMany(market_data_of(code, 1, from_date.strftime('%Y-%m-%d'), {to_date.strftime('%Y-%m-%d')}))
    sortedRawJson = sorted(rawJson, key=operator.attrgetter('timestamp'))
    # info(sortedRawJson)
    find_next_up_gann_square(sortedRawJson,3, 4, 4)
    # [info(jsonData) for jsonData in sortedRawJson]
    # info(rawJson)

    # //rawJson=market_data_of(code, 1, '2017-12-04', '2020-09-06')

    # rawJson = market_data_of(code, 1, '2017-12-04', '2020-09-06')

    # [enrich(x) for x in rawJson]
    # for first in range(20):
    #     for second in range(first):
    #         all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_down_up, (first+1)*10, (second+1)*10, code,
    #                                                          0.0001)
    #         all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_up_down, (first+1)*10, (second+1)*10, code,
    #                                                          0.0001)
    # #all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_up_down, 40, 10, code, 0.0001)

    # all_week_trade_match_result = simulate_week_trade(rawJson)
    # all_month_trade_match_result = simulate_month_trade(rawJson)
    # show_result("date trade", all_date_trade_match_result)
    # show_result("week trade", all_week_trade_match_result)
    # show_result("month trade", all_month_trade_match_result)
