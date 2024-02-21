import datetime
import io
import orjson
import os
import requests

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import pd_utils as pyp
from py_misc_utils import throttle
from py_misc_utils import utils as pyu

from . import api_base
from . import utils as ut


API_NAME = 'MARKETSTACK'

def add_api_options(parser):
  parser.add_argument('--marketstack_key', type=str,
                      help='The MARKETSTACK API key')


def create_api(args):
  return API(api_key=args.marketstack_key, api_rate=args.api_rate)


_QUERY_URL = 'https://api.marketstack.com/v1/intraday'
_TIME_COLUMN = 'date'
_RESP_COLUMNS = {'symbol', 'open', 'high', 'low', 'close', 'volume'}
_DATA_STEPS = {
  '1day': '24hour',
}


def _map_data_step(data_step):
  lowds = data_step.lower()

  return _DATA_STEPS.get(lowds, lowds)


def _issue_request(symbol, **kwargs):
  timeout = kwargs.pop('timeout', pyu.env('FINDATA_TIMEOUT', 90))
  api_key = kwargs.pop('api_key', None)
  params = dict(access_key=api_key)
  params.update(kwargs)

  resp = requests.get(_QUERY_URL, params=params, timeout=timeout)

  tas.check_eq(resp.status_code, 200, msg=f'Request error {resp.status_code}:\n{resp.text}')

  rdata = orjson.loads(resp.text)
  if rdata.data:
    scols = set(rdata.data[0].keys())

    tas.check(all(c in scols for c in _RESP_COLUMNS),
              msg=f'Missing columns: {_RESP_COLUMNS - scols}\nResponse:\n{resp.text}')
    tas.check(_TIME_COLUMN in scols, msg=f'Missing "{_TIME_COLUMN}" column in response data')

  return rdata


def _data_issue_request(**kwargs):
  dtype = kwargs.pop('dtype', np.float32)

  rdata = _issue_request(**kwargs)

  df = pd.DataFrame(rdata.data)

  valid_columns = _RESP_COLUMNS + {_TIME_COLUMN}
  for c in df.columns:
    if c not in valid_columns:
      df.drop(label=c, axis=1, inplace=True)

  df = pyp.type_convert_dataframe(df, {c: dtype for c in _RESP_COLUMNS})
  df.rename(columns={'open': 'o',
                     'close': 'c',
                     'low': 'l',
                     'high': 'h',
                     'volume': 'v',
                     'date': 't'}, inplace=True)

  times = pd.to_datetime(df['t'])
  df['t'] = (times - datetime.datetime(1970, 1, 1)).dt.total_seconds().astype(np.int64)

  alog.debug0(f'Fetched {len(df)} rows from MARKETSTACK for {kwargs.get("symbols").split(",")}')

  return df, rdata


class API(api_base.API):
  # https://marketstack.com/documentation

  def __init__(self, api_key=None, api_rate=None):
    super().__init__()
    self._api_key = api_key or pyu.getenv('MARKETSTACK_KEY')
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)

  @property
  def name(self):
    return 'MarketStack'

  def _get_intraday_data(self, symbols, start_date, end_date, data_step='5Min'):
    dfs, offset = [], 0
    while True:
      alog.debug0(f'Fetching data for {symbols} with {data_step} interval from {start_date} to {end_date}')

      with self._api_throttle.trigger():
        df, rdata = _data_issue_request(symbols=','.join(symbols),
                                        api_key=self._api_key,
                                        interval=_map_data_step(data_step),
                                        date_from=start_date.isoformat(),
                                        date_to=end_date.isoformat(),
                                        offset=offset)

      if not df.empty:
        dfs.append(df)

      pag = rdata['pagination']
      tas.check_eq(pag['count'], len(df))
      offset += pag['count']
      if offset >= pag['total']:
        break

    return dfs

  def fetch_data(self, symbols, start_date=None, end_date=None, data_step='5Min',
                 limit=None, dtype=None):
    alog.debug0(f'Fetch: start={start_date}\tend={end_date}\tlimit={limit}')

    start_date, end_date = ut.infer_time_range(start_date, end_date, data_step,
                                               limit=limit,
                                               tz=pyd.us_eastern_timezone())

    dfs = self._get_intraday_data(symbols, start_date, end_date,
                                  data_step=data_step)

    if dfs:
      df = pd.concat(dfs, ignore_index=True)

      return ut.purge_fetched_data(df, start_date, end_date, limit, data_step)

