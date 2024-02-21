import datetime
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


API_NAME = 'EODHD'

def add_api_options(parser):
  parser.add_argument('--eodhd_key', type=str,
                      help='The EODHD API key')


def create_api(args):
  return API(api_key=args.eodhd_key, api_rate=args.api_rate)


_QUERY_URL = 'https://eodhd.com/api/intraday'
_TIME_COLUMN = 'Timestamp'
_RESP_COLUMNS = {'Open', 'High', 'Low', 'Close', 'Volume'}
_DATA_STEPS = {
  '1min': '1m',
  '5min': '5m',
  '15min': '15m',
  '30min': '30m',
  '60min': '1h',
  '1hour': '1h',
}


def _map_data_step(data_step):
  lowds = data_step.lower()

  return _DATA_STEPS.get(lowds, lowds)


def _issue_request(symbol, **kwargs):
  timeout = kwargs.pop('timeout', pyu.env('FINDATA_TIMEOUT', 90))
  api_key = kwargs.pop('api_key', None)
  params = dict(api_token=api_key, fmt='csv')
  params.update(kwargs)

  resp = requests.get(f'{_QUERY_URL}/{symbol}.US', params=params, timeout=timeout)

  tas.check_eq(resp.status_code, 200, msg=f'Request error {resp.status_code}:\n{resp.text}')

  cols = ut.csv_parse_columns(resp.text)
  scols = set(cols)

  tas.check(all(c in scols for c in _RESP_COLUMNS),
            msg=f'Missing columns: {_RESP_COLUMNS - scols}\nResponse:\n{resp.text}')
  tas.check(_TIME_COLUMN in scols, msg=f'Missing "{_TIME_COLUMN}" column in response data')

  return resp.text, cols


def _data_issue_request(symbol, **kwargs):
  dtype = kwargs.pop('dtype', np.float32)

  data, cols = _issue_request(symbol, **kwargs)

  types = {c: dtype for c in _RESP_COLUMNS}
  types[_TIME_COLUMN] = np.int64

  df = pd.read_csv(io.StringIO(data), dtype=types)
  df.rename(columns={'Open': 'o',
                     'Close': 'c',
                     'Low': 'l',
                     'High': 'h',
                     'Volume': 'v',
                     _TIME_COLUMN: 't'}, inplace=True)
  if symbol:
    df['symbol'] = [symbol] * len(df)

  alog.debug0(f'Fetched {len(df)} rows from EODHD for {symbol}')

  return df


def _enumerate_ranges(start_date, end_date, data_step):
  # Keep this the lowest, as 120 days range is more than enough as iteration step.
  max_range = datetime.timedelta(days=120)

  current_start = start_date
  while True:
    if current_start >= end_date:
      break

    current_end = current_start + max_range
    if current_end > end_date:
      current_end = end_date

    yield current_start, current_end

    current_start = current_end


class API(api_base.API):
  # https://eodhd.com/financial-apis/intraday-historical-data-api

  def __init__(self, api_key=None, api_rate=None):
    super().__init__()
    self._api_key = api_key or pyu.getenv('EODHD_KEY')
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)

  @property
  def name(self):
    return 'EODHD'

  def _get_intraday_data(self, symbols, start_date, end_date, data_step='5Min'):
    dfs = []
    for symbol in symbols:
      alog.debug0(f'Fetching data for {symbol} with {data_step} interval from {start_date} to {end_date}')

      with self._api_throttle.trigger():
        df = _data_issue_request(symbol,
                                 api_key=self._api_key,
                                 interval=_map_data_step(data_step),
                                 **{'from': start_date.timestamp(),
                                    'to': end_date.timestamp()})

      if not df.empty:
        dfs.append(df)

    return dfs

  def fetch_data(self, symbols, start_date=None, end_date=None, data_step='5Min',
                 limit=None, dtype=None):
    alog.debug0(f'Fetch: start={start_date}\tend={end_date}\tlimit={limit}')

    start_date, end_date = ut.infer_time_range(start_date, end_date, data_step,
                                               limit=limit,
                                               tz=pyd.us_eastern_timezone())

    dfs = []
    for range_start, range_end in _enumerate_ranges(start_date, end_date, data_step):
      range_dfs = self._get_intraday_data(symbols, range_start, range_end,
                                          data_step=data_step)
      dfs.extend(range_dfs)

    if dfs:
      df = pd.concat(dfs, ignore_index=True)

      return ut.purge_fetched_data(df, start_date, end_date, limit, data_step)
