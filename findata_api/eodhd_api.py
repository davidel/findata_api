import datetime
import io
import os
import requests
import websocket

import numpy as np
import orjson
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import throttle
from py_misc_utils import utils as pyu

from . import api_base
from . import utils as ut


MODULE_NAME = 'EODHD'

def add_api_options(parser):
  parser.add_argument('--eodhd_key', type=str,
                      help='The EODHD API key')


def create_api(args):
  return API(api_key=args.eodhd_key, api_rate=args.api_rate)


_QUERY_URL = 'https://eodhd.com/api'
_TIME_COLUMNS = {'Timestamp', 'Date'}
_DATE_COLUMNS = {'Date'}
_RESP_COLUMNS = {'Open', 'High', 'Low', 'Close', 'Volume'}
_DATA_STEPS = {
  'min': 'm',
  'hour': 'h',
  'day': 'd',
  'wk': 'w',
  'week': 'w',
  'month': 'mo',
}
_EOD_STEPS = {
  '1d': 'd',
  '1w': 'w',
  '1mo': 'm',
}
_VALID_STEPS = {'1m', '5m', '1h'} | set(_EOD_STEPS.keys())


def _map_data_step(data_step):
  mstep = ut.map_data_step(data_step, _DATA_STEPS)

  tas.check(mstep in _VALID_STEPS,
            msg=f'Invalid data step for EODHD API: "{mstep}" not in {tuple(_VALID_STEPS)}')

  return mstep


def _norm_symbol(symbol):
  return symbol.replace('.', '-')


def _issue_request(symbol, **kwargs):
  timeout = kwargs.pop('timeout', pyu.env('FINDATA_TIMEOUT', 90))
  api_key = kwargs.pop('api_key', None)
  api_kind = kwargs.pop('api_kind', 'intraday')
  params = dict(api_token=api_key, fmt='csv')
  params.update(kwargs)

  req_url = f'{_QUERY_URL}/{api_kind}/{_norm_symbol(symbol)}.US'

  resp = requests.get(req_url, params=params, timeout=timeout)

  tas.check_eq(resp.status_code, 200, msg=f'Request error {resp.status_code}:\n{resp.text}')

  cols = ut.csv_parse_columns(resp.text)
  scols = set(cols)
  if not all(c in scols for c in _RESP_COLUMNS):
    alog.warning(f'Request did not return any results: {req_url} with {params}\n' \
                 f'Response was:\n{resp.text}')
  else:
    time_columns = tuple(scols & _TIME_COLUMNS)
    tas.check(time_columns, msg=f'Missing {_TIME_COLUMNS} column in response data: {cols}')

    return resp.text, cols, time_columns[0]


def _data_issue_request(symbol, **kwargs):
  dtype = kwargs.pop('dtype', np.float32)

  rresp = _issue_request(symbol, **kwargs)
  if rresp is not None:
    data, cols, tcol = rresp

    types = {c: dtype for c in _RESP_COLUMNS}

    df = pd.read_csv(io.StringIO(data), dtype=types)
    df.rename(columns={'Open': 'o',
                       'Close': 'c',
                       'Low': 'l',
                       'High': 'h',
                       'Volume': 'v',
                       tcol: 't'}, inplace=True)
    if symbol:
      df['symbol'] = [symbol] * len(df)
    if tcol in _DATE_COLUMNS:
      df['t'] = ut.convert_to_epoch(df['t'], dtype=np.int64)

    alog.debug0(f'Fetched {len(df)} rows from EODHD for {symbol}')

    return df


def _enumerate_ranges(start_date, end_date, data_step):
  dstep = ut.get_data_step_delta(data_step)
  start_date = pyd.align(start_date, dstep)
  end_date = pyd.align(end_date, dstep, ceil=True)

  if dstep <= datetime.timedelta(minutes=1):
    max_range = datetime.timedelta(days=120)
  elif dstep <= datetime.timedelta(minutes=5):
    max_range = datetime.timedelta(days=600)
  else:
    max_range = datetime.timedelta(days=7200)

  current_start = start_date
  while True:
    if current_start >= end_date:
      break

    current_end = current_start + max_range
    if current_end > end_date:
      current_end = end_date

    yield current_start, current_end

    current_start = current_end


def _time_request_params(start_date, end_date, data_step):
  mstep = _map_data_step(data_step)

  period = _EOD_STEPS.get(mstep, None)
  if period is not None:
    return {
      'api_kind': 'eod',
      'from': start_date.strftime('%Y-%m-%d'),
      'to': end_date.strftime('%Y-%m-%d'),
      'period': period,
    }

  return {
    'api_kind': 'intraday',
    'from': int(start_date.timestamp()),
    'to': int(end_date.timestamp()),
    'interval': mstep,
  }


class WebSocketClient:

  def __init__(self, url, auth_key, process_message, on_close=None, on_error=None):
    self._url = f'{url}?api_token={auth_key}'
    self._run_thread = None
    self._ws = websocket.WebSocketApp(self._url,
                                      on_close=on_close,
                                      on_error=on_error,
                                      on_message=process_message)

  def run(self, **kwargs):
    self._ws.run_forever(**kwargs)

  def run_async(self, **kwargs):
    self._run_thread = threading.Thread(target=self.run, kwargs=kwargs)
    self._run_thread.start()

  def close_connection(self):
    self._ws.close()
    if self._run_thread:
      self._run_thread.join()

  def subscribe(self, *symbols):
    symlist = ','.join(symbols)
    self._ws.send(f'{{"action":"subscribe","symbols":"{symlist}"}}')

  def unsubscribe(self, *symbols):
    symlist = ','.join(symbols)
    self._ws.send(f'{{"action":"unsubscribe","symbols":"{symlist}"}}')


class API(api_base.API):
  # https://eodhd.com/financial-apis/intraday-historical-data-api
  # https://eodhd.com/financial-apis/api-for-historical-data-and-volumes

  def __init__(self, api_key=None, api_rate=None):
    super().__init__(name='EODHD')
    self._api_key = api_key or pyu.getenv('EODHD_KEY')
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)

  def _get_intraday_data(self, symbols, start_date, end_date, data_step):
    dfs = []
    for symbol in symbols:
      alog.debug0(f'Fetching data for {symbol} with {data_step} interval from {start_date} to {end_date}')

      with self._api_throttle.trigger():
        df = _data_issue_request(symbol,
                                 api_key=self._api_key,
                                 **_time_request_params(start_date, end_date, data_step))

      if df is None or df.empty:
        alog.info(f'Missing data for "{symbol}" from {start_date} to {end_date}')
      else:
        dfs.append(df)

    return dfs

  def fetch_data(self, symbols, start_date, end_date, data_step='5Min', dtype=None):
    alog.debug0(f'Fetch: start={start_date}\tend={end_date}')

    dfs = []
    for range_start, range_end in _enumerate_ranges(start_date, end_date, data_step):
      range_dfs = self._get_intraday_data(symbols, range_start, range_end, data_step)
      dfs.extend(range_dfs)

    if dfs:
      df = pd.concat(dfs, ignore_index=True)

      return ut.purge_fetched_data(df, start_date, end_date, data_step)

