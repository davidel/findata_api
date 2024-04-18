import io
import os

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import throttle
from py_misc_utils import utils as pyu

from . import api_base
from . import utils as ut

try:
  import finnhub

  MODULE_NAME = 'FINNHUB'

  def add_api_options(parser):
    parser.add_argument('--finnhub_key', type=str,
                        help='The Finnhub API key')


  def create_api(args):
    return API(api_key=args.finnhub_key, api_rate=args.api_rate)

except ImportError:
  MODULE_NAME = None


_DATA_STEPS = {
  '1min': '1',
  '5min': '5',
  '15min': '15',
  '30min': '30',
  '60min': '60',
  '1d': 'D',
  'day': 'D',
  '1wk': 'W',
  'week': 'W',
  '1mo': 'M',
  'month': 'M',
}


def _map_data_step(data_step):
  step = _DATA_STEPS.get(data_step.lower())
  if not step:
    alog.xraise(RuntimeError, f'Unknown data step: {data_step}')

  return step


class API(api_base.API):
  # https://finnhub.io/docs/api/introduction

  def __init__(self, api_key=None, api_rate=None):
    super().__init__(name='Finnhub')
    self._client = finnhub.Client(api_key=api_key or pyu.getenv('FINNHUB_KEY'))
    self._api_throttle = throttle.Throttle(
      (60 if api_rate is None else api_rate) / 60.0)

  def fetch_single(self, symbol, start_epoch, end_epoch, data_step, dtype=None):
    with self._api_throttle.trigger():
      data = self._client.stock_candles(symbol, _map_data_step(data_step), start_epoch,
                                        end_epoch, format='csv')
    buf = io.StringIO(data)

    types = {c: dtype for c in ['o', 'h', 'l', 'c', 'v']} if dtype else None

    df = pd.read_csv(buf, dtype=types)
    df['symbol'] = [symbol] * len(df)

    alog.debug0(f'Fetched {len(df)} rows from FH for {symbol}')

    return df

  def fetch_data(self, symbols, start_date, end_date, data_step='5Min', dtype=None):
    alog.debug0(f'Fetching from {start_date} to {end_date} symbol {symbols}')

    start_epoch = int(start_date.timestamp())
    end_epoch = int(end_date.timestamp())

    dfs = []
    for symbol in symbols:
      df = self.fetch_single(symbol, start_epoch, end_epoch, data_step, dtype=dtype)
      dfs.append(df)

    df = pd.concat(dfs, ignore_index=True) if dfs else None
    if df is not None:
      df = ut.purge_fetched_data(df, start_date, end_date, data_step)

    return df

