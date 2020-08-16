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

logFormat = '%(asctime)s - %(levelname)s - %(threadName)s - [%(message)s]'
logFormatter = logging.Formatter(logFormat)
logging.basicConfig(filename='test.log', format=logFormat, level=logging.DEBUG)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger = logging.getLogger()
rootLogger.addHandler(consoleHandler)


def marketDataOf(code, interval, start, end):
    urlStr = f"http://fishfish.sytes.net:13000/market_price_asc_view?and=(code.eq.{code},interval_min.eq.{interval},time.gte.{start},time.lte.{end})"
    logging.info(urlStr)
    with urllib.request.urlopen(urlStr) as url:
        data = json.loads(url.read())
        return data


def debugLog(data):
    logging.info(data)


def enrich(data):
    date_time_str = data['time']
    data['timeObj'] = pd.Timestamp(datetime.datetime.fromisoformat(date_time_str))
    data['timeGroup'] = data['timeObj'].round('1d')


def processData(data, dataAfter):
    fail = 0
    match = 0
    sucess = 0
    firstMatch = next(x for x in dataAfter if data['high'] - 20 * 0.0001 >= x['low'])
    debugLog(f"first Data - {data}")
    debugLog(f"First Mathc - {firstMatch}")


if __name__ == '__main__':
    rawJson = marketDataOf('CADUSD', 1, '2019-08-01', '2020-08-07')
    [enrich(x) for x in rawJson]
    groupedData = {k: [data for data in g] for k, g in groupby(sorted(rawJson, key=itemgetter('timeGroup')), key=itemgetter('timeGroup'))}
    #debugLog(groupedData.get(list(groupedData.keys())[0]))
    firstList = groupedData.get(list(groupedData.keys())[0])
    firstData = firstList[0]
    firstDate = firstData['timeObj']
    compareList = [x for x in firstList if x['timeObj'] > firstDate]
    processData(firstData, compareList)





# See PyCharm help at https://www.jetbrains.com/help/pycharm/

