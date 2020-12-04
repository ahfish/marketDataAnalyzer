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


def down_up_strategy(down_pt, up_pt, pt, data):
    if data:
        start_pt = data[0]
        first_date = start_pt['timeObj']
        data_after = [x for x in data if x['timeObj'] > first_date]
        first_match = next((x for x in data_after if start_pt['high'] - down_pt * pt >= x['low']), None)
        debug(
            f"Data - {start_pt['timeObj']}, detect when get lower than {start_pt['high'] - down_pt * pt}")
        if first_match is not None:
            debug(
                f"First Match - {first_match['timeObj']}, try seek higher than {first_match['low'] + up_pt * pt}")
            list_after = [x for x in data_after if x['timeObj'] > first_match['timeObj']]
            second_match = next((x for x in list_after if first_match['low'] + up_pt * pt <= x['high']), None)
            if second_match is not None:
                debug(f"Second Match - {second_match}")
                return MATCH_RESULT.MATCH
            else:
                return MATCH_RESULT.SECOND_UNMATCH
        else:
            return MATCH_RESULT.FIRST_UNMATCH


def simulate_result_with_down_up(data_list, down, up, type, code, duration, pt) -> dict:
    with postgresql() as conn:
        info(f"{inspect.currentframe().f_code.co_name}...")
        first_date = data_list[0]['timeObj']
        last_date = data_list[-1]['timeObj']
        all_match_result = dict.fromkeys(MATCH_RESULT, 0)
        cached_result = conn.result_of(inspect.currentframe().f_code.co_name,
            type,
            code,
            first_date,
            last_date,
            duration,
            down,
            up,
            -1)
        if len(cached_result) > 0:
            all_match_result[MATCH_RESULT.FIRST_UNMATCH] = cached_result[0][0]
            all_match_result[MATCH_RESULT.MATCH] = cached_result[0][1]
            all_match_result[MATCH_RESULT.SECOND_UNMATCH] = cached_result[0][2]
        else:
            result = [down_up_strategy(down, up, pt, [a for a in data_list if a['timeObj'] > x['timeObj']]) for x in data_list]
            for matchType in MATCH_RESULT:
                all_match_result[matchType] = result.count(matchType)
            conn.insert_result(
                inspect.currentframe().f_code.co_name,
                type,
                code,
                first_date,
                last_date,
                duration,
                down,
                up,
                -1,
                all_match_result[MATCH_RESULT.FIRST_UNMATCH],
                all_match_result[MATCH_RESULT.MATCH],
                all_match_result[MATCH_RESULT.SECOND_UNMATCH]
            )
    return all_match_result
    #info("simulate_result_with_down_up..")
    #result = [down_up_strategy(down, up, pt, [a for a in data_list if a['timeObj'] > x['timeObj']]) for x in data_list]
    #all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    #for matchType in MATCH_RESULT:
    #    all_match_result[matchType] = result.count(matchType)
    #return all_match_result


def up_down_strategy(up_pt, down_pt, pt, data):
    if data:
        start_pt = data[0]
        first_date = start_pt['timeObj']
        data_after = [x for x in data if x['timeObj'] > first_date]
        first_match = next((x for x in data_after if start_pt['low'] + up_pt * pt <= x['high']), None)
        debug(
            f"Data - {start_pt['timeObj']}, detect when get higher then {up_pt} * pt - {start_pt['high'] + up_pt * pt}")
        if first_match is not None:
            debug(
                f"First Match - {first_match['timeObj']}, try seek lower than {first_match['high'] - down_pt * pt}")
            list_after = [x for x in data_after if x['timeObj'] > first_match['timeObj']]
            second_match = next((x for x in list_after if first_match['high'] - down_pt * pt >= x['low']), None)
            if second_match is not None:
                debug(f"Second Match - {second_match}")
                return MATCH_RESULT.MATCH
            else:
                return MATCH_RESULT.SECOND_UNMATCH
        else:
            return MATCH_RESULT.FIRST_UNMATCH


def simulate_result_with_up_down(data_list, up, down, type, code, duration, pt) -> dict:
    with postgresql() as conn:
        info(f"{inspect.currentframe().f_code.co_name}...")
        first_date = data_list[0]['timeObj']
        last_date = data_list[-1]['timeObj']
        all_match_result = dict.fromkeys(MATCH_RESULT, 0)
        cached_result = conn.result_of(inspect.currentframe().f_code.co_name,
            type,
            code,
            first_date,
            last_date,
            duration,
            up,
            down,
            -1)
        if len(cached_result) > 0:
            all_match_result[MATCH_RESULT.FIRST_UNMATCH] = cached_result[0][0]
            all_match_result[MATCH_RESULT.MATCH] = cached_result[0][1]
            all_match_result[MATCH_RESULT.SECOND_UNMATCH] = cached_result[0][2]
        else:
            result = [up_down_strategy(up, down, pt, [a for a in data_list if a['timeObj'] > x['timeObj']]) for x in data_list]
            for matchType in MATCH_RESULT:
                all_match_result[matchType] = result.count(matchType)
            conn.insert_result(
                inspect.currentframe().f_code.co_name,
                type,
                code,
                first_date,
                last_date,
                duration,
                up,
                down,
                -1,
                all_match_result[MATCH_RESULT.FIRST_UNMATCH],
                all_match_result[MATCH_RESULT.MATCH],
                all_match_result[MATCH_RESULT.SECOND_UNMATCH]
            )
    return all_match_result


def merge_match_result(final_match_result, match_result):
    for matchType in MATCH_RESULT:
        final_match_result[matchType] += match_result[matchType]


def simulate_day_trade(all_data, func, first_argument, second_argument, code, pt) -> dict:
    with Pool(processes=4) as pool:
        grouped_data = {k: [data for data in g] for k, g in
                        groupby(sorted(all_data, key=itemgetter('timeGroup')), key=itemgetter('timeGroup'))}
        final_all_match_result = dict.fromkeys(MATCH_RESULT, 0)
#        [merge_match_result(final_all_match_result, simulate_result) for simulate_result in
#         [func(date_list, first_argument, second_argument, date_group.strftime("%Y-%m-%d"), code, 'day', pt) for date_group, date_list in grouped_data.items()]]
        [merge_match_result(final_all_match_result, simulate_result) for simulate_result in
         [pool.map(func, (date_list, first_argument, second_argument, date_group.strftime("%Y-%m-%d"), code, 'day', pt,)) for date_group, date_list in grouped_data.items()]]
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
    code = 'AUDUSD'
    rawJson = market_data_of(code, 1, '2017-12-04', '2020-09-06')
    [enrich(x) for x in rawJson]
    for first in range(20):
        for second in range(first):
            all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_down_up, (first+1)*10, (second+1)*10, code,
                                                             0.0001)
            all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_up_down, (first+1)*10, (second+1)*10, code,
                                                             0.0001)
    #all_date_trade_match_result = simulate_day_trade(rawJson, simulate_result_with_up_down, 40, 10, code, 0.0001)

    # all_week_trade_match_result = simulate_week_trade(rawJson)
    # all_month_trade_match_result = simulate_month_trade(rawJson)
    #show_result("date trade", all_date_trade_match_result)
    # show_result("week trade", all_week_trade_match_result)
    # show_result("month trade", all_month_trade_match_result)
