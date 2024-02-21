import datetime
import dateutil
import io
import os
import requests

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import throttle
from py_misc_utils import utils as pyu

from . import api_base
from . import utils as ut


API_NAME = 'ALPHA_VANTAGE'

def add_api_options(parser):
  parser.add_argument('--alpha_vantage_key', type=str,
                      help='The Alpha Vantage API key')


def create_api(args):
  return API(api_key=args.alpha_vantage_key, api_rate=args.api_rate)


_AV_QUERY_URL = 'https://www.alphavantage.co/query'
_TIME_COLUMNS = {'time', 'timestamp'}
_RESP_COLUMNS = {'open', 'high', 'low', 'close', 'volume'}


def _issue_request(func, **kwargs):
  timeout = kwargs.pop('timeout', pyu.env('ALPHA_VANTAGE_TIMEOUT', 90))
  api_key = kwargs.pop('api_key', None)
  params = dict(apikey=api_key, function=func, datatype='csv')
  params.update(kwargs)

  resp = requests.get(_AV_QUERY_URL, params=params, timeout=timeout)

  tas.check_eq(resp.status_code, 200, msg=f'Request error {resp.status_code}:\n{resp.text}')

  cols = ut.csv_parse_columns(resp.text)
  scols = set(cols)

  tas.check(all(c in scols for c in _RESP_COLUMNS),
            msg=f'Missing columns: {_RESP_COLUMNS - scols}\nResponse:\n{resp.text}')

  time_col = None
  for c in _TIME_COLUMNS:
    if c in scols:
      time_col = c
      break

  return resp.text, cols, time_col


def _parse_datetime(s):
  # New York timezone.
  return np.datetime64(dateutil.parser.parse(f'{s} -0400'), 'ms')


def _data_issue_request(func, **kwargs):
  dtype = kwargs.pop('dtype', np.float32)
  symbol = kwargs.get('symbol', None)

  data, cols, time_col = _issue_request(func, **kwargs)

  types = {c: dtype for c in _RESP_COLUMNS}

  df = pd.read_csv(io.StringIO(data),
                   dtype=types,
                   parse_dates=[time_col] if time_col else True)
  df.rename(columns={'open': 'o',
                     'close': 'c',
                     'low': 'l',
                     'high': 'h',
                     'volume': 'v',
                     time_col: 't'}, inplace=True)
  if 't' in df:
    df['t'] = [pyd.np_datetime_to_epoch(_parse_datetime(s)) for s in df['t']]
  if symbol:
    df['symbol'] = [symbol] * len(df)

  alog.debug0(f'Fetched {len(df)} rows from AV for {symbol}')

  return df


def _enumerate_months(start_date, end_date):
  syear, smonth = start_date.year, start_date.month
  eyear, emonth = end_date.year, end_date.month
  year, month = syear, smonth
  while True:
    if year > eyear or (year == eyear and month > emonth):
      break

    yield month, year

    month += 1
    if month > 12:
      month = 1
      year += 1


class API(api_base.API):
  # https://www.alphavantage.co/documentation/#time-series-data

  def __init__(self, api_key=None, api_rate=None):
    super().__init__()
    self._api_key = api_key or pyu.getenv('ALPHA_VANTAGE_KEY')
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)

  @property
  def name(self):
    return 'AlphaVantage'

  def _get_tsi_data(self, symbols, data_step='5Min', month=None):
    dfs = []
    for symbol in symbols:
      alog.debug0(f'Fetching data for {symbol} with {data_step} interval for month {month or "LATEST"}')

      with self._api_throttle.trigger():
        df = _data_issue_request('TIME_SERIES_INTRADAY',
                                 api_key=self._api_key,
                                 symbol=symbol,
                                 interval=data_step.lower(),
                                 month=month,
                                 outputsize='full')
      dfs.append(df)

    return dfs

  def fetch_data(self, symbols, start_date=None, end_date=None, data_step='5Min',
                 limit=None, dtype=None):
    alog.debug0(f'Fetch: start={start_date}\tend={end_date}\tlimit={limit}')

    start_date, end_date = ut.infer_time_range(start_date, end_date, data_step,
                                               limit=limit,
                                               tz=pyd.us_eastern_timezone())

    dfs = []
    for month, year in _enumerate_months(start_date, end_date):
      ymdfs = self._get_tsi_data(symbols,
                                 data_step=data_step,
                                 month=f'{year}-{month:02d}')
      dfs.extend(ymdfs)

    df = pd.concat(dfs, ignore_index=True)

    return ut.purge_fetched_data(df, start_date, end_date, limit, data_step)

