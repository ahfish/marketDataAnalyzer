# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import urllib.request
import json
import logging
import sys


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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    rawJson = marketDataOf('CADUSD', 1, '2019-08-01', '2020-08-07')
    [debugLog(x) for x in rawJson]

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
