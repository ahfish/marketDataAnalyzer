import datetime
import json
import logging
import sys
import urllib.request
from enum import Enum, auto
from itertools import groupby
from operator import itemgetter

import pandas as pd

logFormat = '%(asctime)s - %(levelname)s - %(threadName)s - [%(message)s]'
logFormatter = logging.Formatter(logFormat)
logging.basicConfig(filename='test.log', format=logFormat, level=logging.DEBUG)
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
    logging.info(url_link)
    with urllib.request.urlopen(url_link) as url:
        data = json.loads(url.read())
        return data


def debug_log(data):
    logging.info(data)


def enrich(data):
    date_time_str = data['time']
    data['timeObj'] = pd.Timestamp(datetime.datetime.fromisoformat(date_time_str))
    data['timeGroup'] = data['timeObj'].ceil('-1D')


def process_data(data) -> MATCH_RESULT:
    up_pt = 30
    down_pt = 10
    if data:
        start_pt = data[0]
        first_date = start_pt['timeObj']
        data_after = [x for x in data if x['timeObj'] > first_date]
        first_match = next((x for x in data_after if start_pt['low'] + up_pt * 0.0001 <= x['high']), None)
        debug_log(
            f"Data - {start_pt['timeObj']}, dectect if data['high'] - {up_pt} * 0.0001 - {start_pt['high'] + up_pt * 0.0001}")
        if first_match is not None:
            debug_log(
                f"First Match - {first_match['timeObj']}, try seek lower than {first_match['high'] - down_pt * 0.0001}")
            list_after = [x for x in data_after if x['timeObj'] > first_match['timeObj']]
            second_match = next((x for x in list_after if first_match['high'] - down_pt * 0.0001 >= x['low']), None)
            if second_match is not None:
                debug_log(f"Second Match - {second_match}")
                return MATCH_RESULT.MATCH
            else:
                return MATCH_RESULT.SECOND_UNMATCH
        else:
            return MATCH_RESULT.FIRST_UNMATCH


if __name__ == '__main__':
    rawJson = market_data_of('CADUSD', 1, '2019-08-21', '2020-08-07')
    [enrich(x) for x in rawJson]
    groupedData = {k: [data for data in g] for k, g in
                   groupby(sorted(rawJson, key=itemgetter('timeGroup')), key=itemgetter('timeGroup'))}
    firstList = groupedData.get(list(groupedData.keys())[0])
    result = [process_data([a for a in firstList if a['timeObj'] > x['timeObj']]) for x in firstList]
    all_match_result = dict.fromkeys(MATCH_RESULT, 0)
    for matchType in MATCH_RESULT:
        all_match_result[matchType] = result.count(matchType)
    debug_log(all_match_result)

