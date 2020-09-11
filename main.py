import datetime
import json
import logging
import sys
import urllib.request
from postgresql import *
from enum import Enum, auto
from itertools import groupby
from operator import itemgetter
from multiprocessing import Pool
import pandas as pd
import inspect
import psycopg2
from configparser import ConfigParser

logFormat = '%(asctime)s - %(levelname)s - %(threadName)s - [%(message)s]'
logFormatter = logging.Formatter(logFormat)
# logging.basicConfig(filename='test.log', format=logFormat, level=logging.DEBUG)
logging.basicConfig(filename='test.log', format=logFormat, level=logging.INFO)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger = logging.getLogger()
rootLogger.addHandler(consoleHandler)


class MATCH_RESULT(Enum):
    MATCH = auto()
    FIRST_UNMATCH = auto()
    SECOND_UNMATCH = auto()


def market_data_of(code, interval, start, end):
    url_link = f"http://fishfish.sytes.net:13000/market_price_asc_view?and=(code.eq.{code},interval_min.eq.{interval},time.gte.{start},time.lte.{end})"
    info(url_link)
    with urllib.request.urlopen(url_link) as url:
        data = json.loads(url.read())
        return data


def debug(data):
    logging.debug(data)


def info(data):
    logging.info(data)


def enrich(data):
    date_time_str = data['time']
    date_time = datetime.datetime.fromisoformat(date_time_str)
    data['timeObj'] = pd.Timestamp(date_time)
    data['timeGroup'] = data['timeObj'].ceil('-1D')
    data['weekGroup'] = date_time.strftime("%Y-%U")
    data['monthGroup'] = date_time.strftime("%Y-%m")


def down_up_strategy(down_pt, up_pt, data):
    if data:
        start_pt = data[0]
        first_date = start_pt['timeObj']
        data_after = [x for x in data if x['timeObj'] > first_date]
        first_match = next((x for x in data_after if start_pt['high'] - down_pt * 0.0001 >= x['low']), None)
        debug(
            f"Data - {start_pt['timeObj']}, detect when get lower than {start_pt['high'] - down_pt * 0.0001}")
        if first_match is not None:
            debug(
                f"First Match - {first_match['timeObj']}, try seek higher than {first_match['low'] + up_pt * 0.0001}")
            list_after = [x for x in data_after if x['timeObj'] > first_match['timeObj']]
            second_match = next((x for x in list_after if first_match['low'] + up_pt * 0.0001 <= x['high']), None)
            if second_match is not None:
                debug(f"Second Match - {second_match}")
                return MATCH_RESULT.MATCH
            else:
                return MATCH_RESULT.SECOND_UNMATCH
        else:
            return MATCH_RESULT.FIRST_UNMATCH


def simulate_result_with_down_up(data_list, down, up, code, duration) -> dict:
    info("simulate_result_with_down_up..")
    result = [down_up_strategy(down, up, [a for a in data_list if a['timeObj'] > x['timeObj']]) for x in data_list]
    all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    for matchType in MATCH_RESULT:
        all_match_result[matchType] = result.count(matchType)
    return all_match_result


def up_down_strategy(up_pt, down_pt, data):
    if data:
        start_pt = data[0]
        first_date = start_pt['timeObj']
        data_after = [x for x in data if x['timeObj'] > first_date]
        first_match = next((x for x in data_after if start_pt['low'] + up_pt * 0.0001 <= x['high']), None)
        debug(
            f"Data - {start_pt['timeObj']}, detect when get higher then {up_pt} * 0.0001 - {start_pt['high'] + up_pt * 0.0001}")
        if first_match is not None:
            debug(
                f"First Match - {first_match['timeObj']}, try seek lower than {first_match['high'] - down_pt * 0.0001}")
            list_after = [x for x in data_after if x['timeObj'] > first_match['timeObj']]
            second_match = next((x for x in list_after if first_match['high'] - down_pt * 0.0001 >= x['low']), None)
            if second_match is not None:
                debug(f"Second Match - {second_match}")
                return MATCH_RESULT.MATCH
            else:
                return MATCH_RESULT.SECOND_UNMATCH
        else:
            return MATCH_RESULT.FIRST_UNMATCH


def simulate_result_with_up_down(data_list, up, down, type, code, duration) -> dict:
    info(f"{inspect.currentframe().f_code.co_name}...")
    result = [up_down_strategy(up, down, [a for a in data_list if a['timeObj'] > x['timeObj']]) for x in data_list]
    all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    for matchType in MATCH_RESULT:
        all_match_result[matchType] = result.count(matchType)

    first_date = data_list[0]['timeObj']
    last_date = data_list[-1]['timeObj']
    with postgresql() as conn:
        conn.insert_result(
            inspect.currentframe().f_code.co_name,
            type,
            code,
            first_date,
            last_date,
            duration,
            up,
            down,
            None,
            all_match_result[MATCH_RESULT.FIRST_UNMATCH],
            all_match_result[MATCH_RESULT.MATCH],
            all_match_result[MATCH_RESULT.SECOND_UNMATCH]
        )
    return all_match_result


def merge_match_result(final_match_result, match_result):
    for matchType in MATCH_RESULT:
        final_match_result[matchType] += match_result[matchType]


def simulate_day_trade(all_data, func, first_argument, second_argument, code) -> dict:
    grouped_data = {k: [data for data in g] for k, g in
                    groupby(sorted(all_data, key=itemgetter('timeGroup')), key=itemgetter('timeGroup'))}
    final_all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    [merge_match_result(final_all_match_result, simulate_result) for simulate_result in
     [func(date_list, first_argument, second_argument, date_group.strftime("%Y-%m-%d"), code, 'day') for date_group, date_list in grouped_data.items()]]
    return final_all_match_result


def simulate_week_trade(all_data, func, first_argument, second_argument, code) -> dict:
    grouped_data = {k: [data for data in g] for k, g in
                    groupby(sorted(all_data, key=itemgetter('weekGroup')), key=itemgetter('weekGroup'))}
    final_all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    [merge_match_result(final_all_match_result, simulate_result) for simulate_result in
     [func(date_list, first_argument, second_argument, 'week') for date_group, date_list in grouped_data.items()]]
    return final_all_match_result


def simulate_month_trade(all_data, func, first_argument, second_argument, code) -> dict:
    grouped_data = {k: [data for data in g] for k, g in
                    groupby(sorted(all_data, key=itemgetter('monthGroup')), key=itemgetter('monthGroup'))}
    final_all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    [merge_match_result(final_all_match_result, simulate_result) for simulate_result in
     [func(date_list, first_argument, second_argument, 'month') for date_group, date_list in grouped_data.items()]]
    return final_all_match_result


def show_result(type, result):
    info(
        f"{type} - {result}, result = {result[MATCH_RESULT.MATCH] / result[MATCH_RESULT.SECOND_UNMATCH]}")


if __name__ == '__main__':
    code = 'CADUSD'
    rawJson = market_data_of(code, 1, '2019-08-21', '2020-08-07')
    [enrich(x) for x in rawJson]
    all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_up_down, 40, 10, code)
    # all_week_trade_match_result = simulate_week_trade(rawJson)
    # all_month_trade_match_result = simulate_month_trade(rawJson)
    show_result("date trade", all_date_trade_match_result)
    # show_result("week trade", all_week_trade_match_result)
    # show_result("month trade", all_month_trade_match_result)
