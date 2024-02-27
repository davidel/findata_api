import asyncio
import datetime
import os
import threading
import time

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import pd_utils as pyp
from py_misc_utils import throttle
from py_misc_utils import utils as pyu

from . import api_base
from . import api_types
from . import utils as ut

try:
  import alpaca_trade_api as alpaca

  MODULE_NAME = 'ALPACA'

  def add_api_options(parser):
    parser.add_argument('--alpaca_key', type=str,
                        help='The Alpaca API key')
    parser.add_argument('--alpaca_secret', type=str,
                        help='The Alpaca API secret')
    parser.add_argument('--alpaca_url', type=str,
                        help='The Alpaca API base URL')

  def create_api(args):
    return API(api_key=args.alpaca_key, api_secret=args.alpaca_secret,
               api_url=args.alpaca_url, api_rate=args.api_rate)

except ImportError:
  MODULE_NAME = None


_DATA_STEPS = {
  '1min': '1Min',
  'minute': '1Min',
  '5min': '5Min',
  '15min': '15Min',
  '1d': '1D',
  '1day': '1D',
  'day': '1D',
}
_FETCH_ORDERS_MAX = 500


def _map_data_step(data_step):
  step = _DATA_STEPS.get(data_step.lower(), None)
  if not step:
    alog.xraise(RuntimeError, f'Unknown data step: {data_step}')

  return step


def _get_config(key, secret, url):
  if key is None:
    key = pyu.getenv('APCA_API_KEY_ID')
  if secret is None:
    secret = pyu.getenv('APCA_API_SECRET_KEY')
  if url is None:
    url = pyu.getenv('APCA_API_BASE_URL')
    if not url:
      url = 'https://paper-api.alpaca.markets'

  alog.debug0(f'Alpaca API created with: key={key} secret={secret} url={url}')

  return key, secret, url


def _get_df_from_bars(bars, dtype=None):
  df_rows = []
  for bar in bars:
    row = dict(bar.__dict__.get('_raw'))
    # Rename 'S' to Tradzy standard 'symbol'.
    row['symbol'] = row['S']
    row.pop('S')

    # Convert time string to EPOCH timestamp.
    row['t'] = pyd.parse_date(row['t']).timestamp()

    df_rows.append(row)

  df = pd.DataFrame(df_rows)
  if dtype:
    for c in pyp.get_df_columns(df, discards={'t', 'symbol'}):
      df[c] = df[c].astype(dtype)

  return df


def _maybe_date(dstr):
  return pyd.parse_date(str(dstr)) if dstr else None


def _marshal_order(o):
  return api_types.Order(id=o.id,
                         symbol=o.symbol,
                         quantity=pyu.cast(o.qty, float),
                         side=o.side,
                         type=o.type,
                         limit=pyu.cast(o.limit_price, float),
                         stop=pyu.cast(o.stop_price, float),
                         status=o.status,
                         created=_maybe_date(o.created_at),
                         filled=_maybe_date(o.filled_at),
                         filled_quantity=pyu.cast(o.filled_qty, float),
                         filled_avg_price=pyu.cast(o.filled_avg_price, float))


def _marshal_position(p):
  return api_types.Position(symbol=p.symbol,
                            quantity=pyu.cast(p.qty, float),
                            value=pyu.cast(p.market_value, float))


def _marshal_account(a):
  return api_types.Account(id=a.account_number,
                           buying_power=pyu.cast(a.buying_power, float))


def _get_stream_ts(v):
  return v.seconds + v.nanoseconds * 1e-9


def _marshal_stream_trade(t):
  return api_types.StreamTrade(timestamp=_get_stream_ts(t['t']),
                               symbol=t['S'],
                               quantity=t['s'],
                               price=t['p'])


def _marshal_stream_quote(q):
  return api_types.StreamQuote(timestamp=_get_stream_ts(q['t']),
                               symbol=q['S'],
                               bid_size=q['bs'],
                               bid_price=q['bp'],
                               ask_size=q['as'],
                               ask_price=q['ap'])


class Stream:

  def __init__(self, api_key, api_secret,
               data_stream_url='https://stream.data.alpaca.markets',
               data_feed='sip'):
    self._conn = alpaca.stream.Stream(
      api_key,
      api_secret,
      data_stream_url=data_stream_url,
      raw_data=True,
      data_feed=data_feed)

    self._stream_thread = None
    self._trade_handler = None
    self._quote_handler = None
    self._symbols = None
    self._lock = threading.Lock()
    self._thread_loop = None
    self._stopping = False

    self._stream_thread = threading.Thread(target=self._stream_thread_fn, daemon=True)
    self._stream_thread.start()

  def __del__(self):
    self.stop()

  async def _stream_handler(self, d):
    with self._lock:
      trade_handler, quote_handler = self._trade_handler, self._quote_handler

    kind = d.get('T', None)
    if kind == 'q':
      if quote_handler is not None:
        quote_handler(_marshal_stream_quote(d))
    elif kind == 't':
      if trade_handler is not None:
        trade_handler(_marshal_stream_trade(d))

  def _stream_thread_fn(self):
    self._thread_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(self._thread_loop)
    try:
      self._conn.run()
    except Exception as e:
      alog.exception(e, exmsg=f'Exception while running the stream thread loop')
    alog.info(f'Stream thread exiting run loop')

  def stop(self):
    if not self._stopping:
      self._stopping = True
      asyncio.run_coroutine_threadsafe(self._conn.stop_ws(), self._thread_loop)
      self._stream_thread.join()

  def register(self, symbols, trade_handler=None, quote_handler=None):
    with self._lock:
      if self._symbols:
        self._conn.unsubscribe_trades(*self._symbols)
        self._conn.unsubscribe_quotes(*self._symbols)
        self._symbols = None

      self._trade_handler, self._quote_handler = trade_handler, quote_handler

      if symbols:
        self._conn.subscribe_trades(self._stream_handler, *symbols)
        self._conn.subscribe_quotes(self._stream_handler, *symbols)
        self._symbols = list(symbols)



class API(api_base.API):

  def __init__(self, api_key=None, api_secret=None, api_url=None, api_rate=None,
               symbols_per_step=20, data_stream_url='https://stream.data.alpaca.markets',
               data_feed='sip'):
    super().__init__()
    self._api_key, self._api_secret, self._api_url = _get_config(api_key, api_secret, api_url)
    self._api = alpaca.REST(self._api_key, self._api_secret, self._api_url)
    self._api_throttle = throttle.Throttle(
      (200 if api_rate is None else api_rate) / 60.0)
    self._symbols_per_step = symbols_per_step
    self._data_stream_url = data_stream_url
    self._data_feed = data_feed
    self._stream = None

  @property
  def name(self):
    return 'Alpaca'

  @property
  def supports_streaming(self):
    return True

  @property
  def supports_trading(self):
    return True

  def register_stream_handlers(self, symbols, trade_handler=None, quote_handler=None):
    if self._stream is not None:
      alog.debug1(f'Stopping previous real time stream')
      self._stream.stop()

    if symbols:
      alog.debug1(f'Creating new real time stream for {symbols}')
      self._stream = Stream(self._api_key, self._api_secret,
                            data_stream_url=self._data_stream_url,
                            data_feed=self._data_feed)
      self._stream.register(symbols, trade_handler=trade_handler, quote_handler=quote_handler)
    else:
      self._stream = None

  def get_account(self):
    with self._api_throttle.trigger():
      account = self._api.get_account()

    return _marshal_account(account)

  def get_market_hours(self, dt):
    dtz = dt.astimezone(pyd.us_eastern_timezone())
    dts = dtz.strftime('%Y-%m-%d')
    with self._api_throttle.trigger():
      calendar = self._api.get_calendar(start=dts, end=dts)
    if calendar:
      calendar = calendar[0]
      market_open = dtz.replace(hour=calendar.open.hour, minute=calendar.open.minute,
                                second=0, microsecond=0)
      market_close = dtz.replace(hour=calendar.close.hour, minute=calendar.close.minute,
                                 second=0, microsecond=0)

      return market_open, market_close

  def submit_order(self, symbol, quantity, side, type, limit=None, stop=None):
    with self._api_throttle.trigger():
      order = self._api.submit_order(symbol, qty=quantity, side=side, type=type,
                                     limit_price=limit, stop_price=stop)

    return _marshal_order(order)

  def get_order(self, oid):
    with self._api_throttle.trigger():
      order = self._api.get_order(oid)

    return _marshal_order(order)

  def _fetch_orders(self, limit=None, status='all', start_date=None, end_date=None):
    after = start_date.isoformat() if start_date else None
    until = end_date.isoformat() if end_date else None
    with self._api_throttle.trigger():
      orders = self._api.list_orders(limit=limit or _FETCH_ORDERS_MAX,
                                     status=status,
                                     after=after,
                                     until=until)

    return [_marshal_order(o) for o in orders]

  def _dedup_timefilter_orders(self, orders, start_date=None, end_date=None):
    od = dict()
    for order in orders:
      if start_date is not None and order.created < start_date:
        continue
      if end_date is not None and order.created > end_date:
        continue
      od[order.id] = order

    return sorted(od.values(), key=lambda x: x.created)

  def list_orders(self, limit=None, status='all', start_date=None, end_date=None):
    if end_date is None:
      end_date = pyd.now()
    if start_date is None:
      start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    edate = end_date
    orders = []
    while True:
      xorders = self._fetch_orders(limit=_FETCH_ORDERS_MAX, status=status,
                                   start_date=start_date, end_date=edate)
      orders.extend(xorders)
      if len(xorders) < _FETCH_ORDERS_MAX:
        break
      orders = sorted(orders, key=lambda x: x.created)
      edate = orders[0].created + datetime.timedelta(seconds=1)
      if edate <= start_date:
        break

    orders = self._dedup_timefilter_orders(orders,
                                           start_date=start_date,
                                           end_date=end_date)

    return orders if limit is None else orders[-limit:]

  def cancel_order(self, oid):
    with self._api_throttle.trigger():
      self._api.cancel_order(oid)

  def list_positions(self):
    with self._api_throttle.trigger():
      positions = self._api.list_positions()

    return [_marshal_position(p) for p in positions]

  def fetch_data(self, symbols, start_date=None, end_date=None, data_step='5Min',
                 limit=None, dtype=None):
    step_delta = ut.get_data_step_delta(data_step)
    start_date, end_date = ut.infer_time_range(start_date, end_date, step_delta,
                                               limit=limit,
                                               tz=pyd.us_eastern_timezone())

    # At Alpaca level API we need to use an hard limit, and break time range.
    tsteps = ut.break_period_in_dates_list(start_date, end_date, step_delta, limit or 1000)
    dfs = []
    for ts in tsteps:
      start = ts.start.isoformat() if ts.start else None
      end = ts.end.isoformat() if ts.end else None

      alog.debug0(f'Fetch: start={start or "*"}\tend={end or "*"}\tlimit={ts.limit}')
      for srange in range(0, len(symbols), self._symbols_per_step):
        step_symbols = symbols[srange: srange + self._symbols_per_step]
        with self._api_throttle.trigger():
          bars = self._api.get_bars(step_symbols, _map_data_step(data_step),
                                      limit=ts.limit,
                                      start=start,
                                      end=end)
        bsdf = _get_df_from_bars(bars, dtype=dtype)
        if not bsdf.empty:
          dfs.append(bsdf)

    df = pd.concat(dfs, ignore_index=True) if dfs else None
    if df is not None:
      df = ut.purge_fetched_data(df, start_date, end_date, limit, step_delta)

    alog.debug0(f'Fetched {len(df) if df is not None else 0} records')

    return df
