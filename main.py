# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import urllib.request
import json
import logging
import sys
import datetime
import pandas as pd
from operator import itemgetter
from operator import attrgetter
from itertools import groupby
from enum import Enum, auto

logFormat = '%(asctime)s - %(levelname)s - %(threadName)s - [%(message)s]'
logFormatter = logging.Formatter(logFormat)
logging.basicConfig(filename='test.log', format=logFormat, level=logging.DEBUG)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger = logging.getLogger()
rootLogger.addHandler(consoleHandler)


class MATCH_RESULT(Enum):
    MATCH = auto()
    FIRST_MATCH_ONLY = auto()
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
    upPt = 10
    downPt = 10
    if data:
        startPt = data[0]
        firstDate = startPt['timeObj']
        dataAfter = [x for x in data if x['timeObj'] > firstDate]
        firstMatch = next((x for x in dataAfter if startPt['low'] + upPt * 0.0001 <= x['high']), None)
        debug_log(
            f"Data - {startPt['timeObj']}, dectect if data['high'] - {upPt} * 0.0001 - {startPt['high'] + upPt * 0.0001}")
        if firstMatch is not None:
            debug_log(
                f"First Match - {firstMatch['timeObj']}, try seek lower than {firstMatch['high'] - downPt * 0.0001}")
            list_after = [x for x in dataAfter if x['timeObj'] > firstMatch['timeObj']]
            second_match = next((x for x in list_after if firstMatch['high'] - downPt * 0.0001 >= x['low']), None)
            if second_match is not None:
                debug_log(f"Second Match - {second_match}")
                return MATCH_RESULT.MATCH
            else:
                return MATCH_RESULT.SECOND_UNMATCH
        else:
            return MATCH_RESULT.FIRST_MATCH_ONLY


if __name__ == '__main__':
    rawJson = market_data_of('CADUSD', 1, '2019-08-21', '2020-08-07')
    [enrich(x) for x in rawJson]
    groupedData = {k: [data for data in g] for k, g in
                   groupby(sorted(rawJson, key=itemgetter('timeGroup')), key=itemgetter('timeGroup'))}
    firstList = groupedData.get(list(groupedData.keys())[0])
    result = [process_data([a for a in firstList if a['timeObj'] > x['timeObj']]) for x in firstList]
    debug_log(result)
