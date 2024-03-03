import datetime
import numpy as np
import pandas as pd
import pytz

from py_misc_utils import alog
from py_misc_utils import date_utils as pyd
from py_misc_utils import throttle

from . import api_base
from . import utils as ut

try:
  import yfinance as yf

  MODULE_NAME = 'YFINANCE'

  def add_api_options(parser):
    pass

  def create_api(args):
    return API(api_rate=args.api_rate)

except ImportError:
  MODULE_NAME = None


_COLUMNS = ['Close', 'Open', 'Low', 'High', 'Volume']
_BAR_RENAMES = {
  'Open': 'o',
  'Close': 'c',
  'Low': 'l',
  'High': 'h',
  'Volume': 'v'
}
_DATA_STEPS = {
  'min': 'm',
  'day': 'd',
  'week': 'wk',
  'month': 'mo',
}


def _get_yahoo_date(dt):
  # We use US/Eastern timestamps, and YFinance wants localtime as it does:
  #  int(time.mktime(dt.timetuple()))
  #
  # https://github.com/ranaroussi/yfinance/blob/main/yfinance/base.py
  return dt.astimezone()


class API(api_base.API):
  # https://github.com/ranaroussi/yfinance

  def __init__(self, api_rate=None):
    super().__init__()
    self._api_throttle = throttle.Throttle(
      (30 if api_rate is None else api_rate) / 60.0)

  @property
  def name(self):
    return 'YFinance'

  def range_supported(self, start_date, end_date, data_step):
    ds_delta = ut.get_data_step_delta(data_step)
    now = pyd.now(tz=start_date.tzinfo)
    if ds_delta >= datetime.timedelta(days=1):
      # Days candels can go back in time.
      return True
    if ds_delta <= datetime.timedelta(minutes=1):
      return (now - start_date) <= datetime.timedelta(days=30)

    # Anything within days must be within 60 days.
    return (now - start_date) < datetime.timedelta(days=60)

  def get_data(self, tkr, symbol, start_date, end_date, interval, dtype=None):
    with self._api_throttle.trigger():
      df = tkr.history(start=start_date, end=end_date, interval=interval)
    df = df[_COLUMNS].copy()

    df.rename(columns=_BAR_RENAMES, inplace=True)
    df['symbol'] = [symbol] * len(df)

    # The API returns a DataFrame with the time index. We set the name here,
    # and then let reset_index() to move it into a column.
    df.index.name = 't'
    df = df.reset_index()
    df['t'] = ut.get_dataframe_index_time(df, col='t')

    if dtype is not None:
      dtypes = {c: dtype for c in ('c', 'o', 'l', 'h', 'v')}
      df = df.astype(dtype=dtypes)

    return df

  def _fetch_single(self, symbol, start_date, end_date, data_step='5Min', dtype=None):
    alog.debug0(f'Fetching from {start_date} to {end_date} symbol {symbol}')

    interval = ut.map_data_step(data_step, _DATA_STEPS)
    ystart_date = _get_yahoo_date(start_date)
    yend_date = _get_yahoo_date(end_date)

    tkr = yf.Ticker(symbol)

    if ut.get_data_step_delta(data_step) < datetime.timedelta(minutes=5):
      # Anything less than 5 minutes needs to be broken up in 7 days chunks.
      delta = datetime.timedelta(days=7)
      start = ystart_date
      dfs = []
      while start < yend_date:
        end = start + delta
        if end > yend_date:
          end = yend_date

        df = self.get_data(tkr, symbol, start, end, interval, dtype=dtype)
        dfs.append(df)

        start += delta

      df = pd.concat(dfs, ignore_index=True)
    else:
      df = self.get_data(tkr, symbol, ystart_date, yend_date, interval, dtype=dtype)

    alog.debug0(f'Fetched {len(df)} rows from YF for {symbol}')

    return df

  def fetch_data(self, symbols, start_date, end_date, data_step='5Min', dtype=None):
    dfs = []
    for symbol in symbols:
      df = self._fetch_single(symbol, start_date, end_date,
                              data_step=data_step,
                              dtype=dtype)
      dfs.append(df)

    df = pd.concat(dfs, ignore_index=True) if dfs else None
    if df is not None:
      df = ut.purge_fetched_data(df, start_date, end_date, data_step)

    return df

