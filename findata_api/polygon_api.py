import collections
import datetime
import os
import re
import socket
import threading

import orjson
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
  import polygon
  import websocket

  MODULE_NAME = 'POLYGON'

  def add_api_options(parser):
    parser.add_argument('--polygon_key', type=str,
                        help='The Polygon API key')


  def create_api(args):
    return API(api_key=args.polygon_key, api_rate=args.api_rate)

except ImportError:
  MODULE_NAME = None


_DATA_STEPS = {
  'min': 'minute',
  'h': 'hour',
  'd': 'day',
  'w': 'week',
}
_BAR_COLUMNS = set(os.getenv('POLYGON_BAR_COLUMNS', 't,o,c,h,l,n,v,vw').split(','))
_BAR_RENAMES = {'vw': 'a'}
_MAX_BASE_BARS = 50000
_DEFAULT_LIMIT = 2000


def _map_data_step(data_step):
  m = re.match(r'(\d*)([a-z]+)', data_step.lower())
  if not m:
    alog.xraise(RuntimeError, f'Unknown data step: {data_step}')

  mult = int(m.group(1)) if m.group(1) else 1
  span = _DATA_STEPS.get(m.group(2), m.group(2))

  return mult, span


def _get_config(key):
  if key is None:
    key = pyu.getenv('POLYGON_API_KEY')
    tas.check_is_not_none(key, msg=f'Polygon API key not specified')

  alog.debug0(f'Polygon API created with: key={key}')

  return key


def _get_df_from_response(symbol, resp, dtype=None):
  results = getattr(resp, 'results', None)
  if results:
    df = pd.DataFrame(results)

    drop_cols = [c for c in df.columns if c not in _BAR_COLUMNS]
    if drop_cols:
      df.drop(drop_cols, axis=1, inplace=True)
    df.rename(columns=_BAR_RENAMES, inplace=True)

    df['symbol'] = [symbol] * len(df)
    df['t'] = df['t'] // 1000
    if dtype:
      for c in pyp.get_df_columns(df, discards={'t', 'symbol'}):
        df[c] = df[c].astype(dtype)

    return df


def _get_stream_ts(v):
  return v / 1000


def _marshal_stream_trade(t):
  return api_types.StreamTrade(timestamp=_get_stream_ts(t['t']),
                               symbol=t['sym'],
                               quantity=t['s'],
                               price=t['p'])


def _marshal_stream_quote(q):
  return api_types.StreamQuote(timestamp=_get_stream_ts(q['t']),
                               symbol=q['sym'],
                               bid_size=q['bs'],
                               bid_price=q['bp'],
                               ask_size=q['as'],
                               ask_price=q['ap'])


def _marshal_stream_bar(b):
  return api_types.StreamBar(timestamp=_get_stream_ts(b['s']),
                             symbol=b['sym'],
                             open=b['o'],
                             high=b['h'],
                             low=b['l'],
                             close=b['c'],
                             volume=b['v'])


def _sublist(symbols, handlers):
  sub = []
  for sym in symbols:
    if handlers.get('trades', None) is not None:
      sub.append(f'T.{sym}')
    if handlers.get('quotes', None) is not None:
      sub.append(f'Q.{sym}')
    if handlers.get('bars', None) is not None:
      sub.append(f'A.{sym}')

  return sub


_STOCKS = 'stocks'
_FOREX = 'forex'
_CRYPTO = 'crypto'

def _ws_url(cluster, service=None):
  return f'wss://{service or "socket"}.polygon.io/{cluster}'


class WebSocketClient:
  def __init__(self, url, auth_key, process_message,
               service=None,
               on_close=None,
               on_error=None):
    self._url = url
    self._auth_key = auth_key

    self._ws = websocket.WebSocketApp(self._url,
                                      on_close=on_close,
                                      on_error=on_error,
                                      on_message=process_message)

    self._run_thread = None

  def run(self, **kwargs):
    self._ws.run_forever(**kwargs)

  def run_async(self, **kwargs):
    self._run_thread = threading.Thread(target=self.run, kwargs=kwargs)
    self._run_thread.start()

  def close_connection(self):
    self._ws.close()
    if self._run_thread:
      self._run_thread.join()

  def subscribe(self, *params):
    fparams = ','.join(params)
    self._ws.send(f'{{"action":"subscribe","params":"{fparams}"}}')

  def unsubscribe(self, *params):
    fparams = ','.join(params)
    self._ws.send(f'{{"action":"unsubscribe","params":"{fparams}"}}')

  def authenticate(self):
    self._ws.send(f'{{"action":"auth","params":"{self._auth_key}"}}')


class Stream:

  DEFAULT_CTX = dict(
    handlers=dict(),
    started=False,
    ws_symbols=(),
  )

  SOCKET_BUFFER_SIZE = 8 * 1024 * 1024

  def __init__(self, api_key):
    self._api_key = api_key
    self._lock = threading.Lock()
    self._status_cv = threading.Condition(self._lock)
    self._status = collections.defaultdict(list, CLOSED=[])
    self._ctx = Stream._make_ctx()
    self._ws_api = WebSocketClient(_ws_url(_STOCKS, service=pyu.getenv('POLYGON_SERVICE')),
                                   self._api_key,
                                   self._process_message,
                                   service=pyu.getenv('POLYGON_SERVICE'),
                                   on_close=self._on_close,
                                   on_error=self._on_error)

  def _wait_status(self, status, timeout=None):
    with self._status_cv:
      if status not in self._status:
        self._status_cv.wait()

      return self._status.get(status, None)

  def _set_status(self, status, meta=None, add=True):
    with self._status_cv:
      if not add:
        self._status.clear()

      if meta is not None:
        self._status[status].append(meta)
      else:
        self._status[status] = []

      self._status_cv.notify()

  def _has_status(self, status):
    with self._lock:
      return status in self._status

  @staticmethod
  def _make_ctx(**kwargs):
    args = Stream.DEFAULT_CTX.copy()
    args.update(**kwargs)

    return pyu.make_object(**args)

  def _new_ctx(self, **kwargs):
    ctx = self._ctx
    args = {k: getattr(ctx, k) for k in Stream.DEFAULT_CTX.keys()}
    args.update(**kwargs)

    nctx = pyu.make_object(**args)
    self._ctx = nctx

    return nctx

  def _reconnect(self):
    with self._lock:
      alog.info(f'Reconnecting Polygon WebSocket')

      self._stop()
      self._start()

      ctx = self._ctx
      if ctx.ws_symbols:
        # Prevent _register() from unregistering existing symbols, as the connection
        # is not the same they were registered into.
        self._new_ctx(ws_symbols=())
        self._register(ctx.ws_symbols, ctx.handlers)

  def _start(self):
    alog.debug2(f'Starting Polygon WebSocket connection')

    # We bump up the WebSocket buffer size to avoid message traffic peaks to
    # cause back pressure on the server side, which will result in the server
    # dropping the connection.
    buffer_size = pyu.getenv('WEBSOCK_BUFSIZE', dtype=int,
                             defval=Stream.SOCKET_BUFFER_SIZE)
    alog.debug0(f'Using WebSocket receive buffer of {buffer_size} bytes')

    socket_options = (
      (socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_size),
    )

    self._ws_api.run_async(sockopt=socket_options)
    self._new_ctx(started=True)

  def start(self):
    with self._lock:
      self._start()

  def _stop(self):
    alog.debug2(f'Stopping Polygon WebSocket connection')
    self._new_ctx(started=False)
    self._ws_api.close_connection()

  def stop(self):
    with self._lock:
      self._stop()

    self._set_status('CLOSED', add=False)

  def _on_close(self, wsa, status, msg):
    ctx = self._ctx
    alog.warning(f'Streaming connection closed: ({status}) {msg}')
    if ctx.started:
      # Cannot do the reconnect work from inside the WebSocket callback. Just spawn
      # an async thread and do it from there. The WebSocket API will call the on-close
      # callback one all the teardown is completed, making it safe to issue a new
      # run_async().
      pyu.run_async(pyu.xwrap_fn(self._reconnect))

  def _on_error(self, wsa, error):
    alog.error(f'Streaming connection error: {error}')

  def _process_message(self, wsa, msg):
    # Code within WebSocket callbacks should not take the lock. All the data needed
    # by the callback is stored within the context, whose content is never modified
    # (it gets just replaced with a new copy).
    ctx = self._ctx

    data = orjson.loads(msg)
    for d in data:
      kind = d.get('ev', None)
      if kind == 'Q':
        handler = ctx.handlers.get('quotes', None)
        if handler is not None:
          handler(_marshal_stream_quote(d))
      elif kind == 'T':
        handler = ctx.handlers.get('trades', None)
        if handler is not None:
          handler(_marshal_stream_trade(d))
      elif kind[0] == 'A':
        handler = ctx.handlers.get('bars', None)
        if handler is not None:
          handler(_marshal_stream_bar(d))
      elif kind == 'status':
        alog.debug0(f'Status Message: {d}')

        status = d.get('status', None)
        if status == 'auth_success':
          self._set_status('AUTHENTICATED')
        elif status == 'connected':
          self._set_status('CONNECTED')
        elif status == 'error':
          self._set_status('ERROR', meta=d.get('message', None))
      else:
        alog.debug0(f'Stream Message: {d}')

  def authenticate(self):
    if not self._has_status('AUTHENTICATED'):
      alog.debug0(f'Authenticating to the Polygon streaming service')
      self._wait_status('CONNECTED')
      self._ws_api.authenticate()
      self._wait_status('AUTHENTICATED')

  def _register(self, symbols, handlers):
    ctx = self._ctx
    unsub = _sublist(ctx.ws_symbols, handlers)
    if unsub:
      self._ws_api.unsubscribe(*unsub)

    sub = _sublist(symbols, handlers)
    if sub:
      self._ws_api.subscribe(*sub)

    self._new_ctx(ws_symbols=tuple(symbols), handlers=handlers)

  def register(self, symbols, handlers):
    with self._lock:
      self._register(symbols, handlers)


class API(api_base.API):

  def __init__(self, api_key=None, api_rate=None):
    super().__init__()
    self._api_key = _get_config(api_key)
    self._api = polygon.RESTClient(self._api_key)
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)
    self._stream = None

  def __del__(self):
    if self._stream is not None:
      self._stream.stop()

  @property
  def name(self):
    return 'Polygon'

  @property
  def supports_streaming(self):
    return True

  def register_stream_handlers(self, symbols, handlers):
    if self._stream is None and symbols:
      self._stream = Stream(self._api_key)
      self._stream.start()
      self._stream.authenticate()

    alog.debug1(f'Registering Streaming: handlers={tuple(handlers.keys())}\tsymbols={symbols}')

    self._stream.register(symbols, handlers)

    alog.debug1(f'Registration done!')

    if self._stream is not None and not symbols:
      self._stream.stop()
      self._stream = None

  def _get_limited_limit(self, span, step_delta, max_bars):
    limit = pyu.getenv('POLYGON_LIMIT', dtype=int, defval=_DEFAULT_LIMIT)

    if span in {'minute', 'hour'}:
      bar_limit = max_bars // int(step_delta.total_seconds() / 60)
    else:
      bar_limit = max_bars // int(step_delta.total_seconds() / 86400)

    return min(limit, max(bar_limit, 1))

  def fetch_data(self, symbols, start_date, end_date, data_step='5Min', dtype=None):
    mult, span = _map_data_step(data_step)
    step_delta = ut.get_data_step_delta(data_step)

    start_date = pyd.align(start_date, step_delta)

    xlimit = self._get_limited_limit(span, step_delta, _MAX_BASE_BARS)
    tsteps = ut.break_period_in_dates_list(start_date, end_date, step_delta * xlimit)

    dfs = []
    for tstart, tend in tsteps:
      start = int(tstart.timestamp() * 1000)
      end = int(tend.timestamp() * 1000)

      alog.debug0(f'Fetch: start={tstart}\tend={tend}')
      for symbol in symbols:
        with self._api_throttle.trigger():
          resp = self._api.stocks_equities_aggregates(symbol,
                                                      mult,
                                                      span,
                                                      start,
                                                      end,
                                                      limit=_MAX_BASE_BARS,
                                                      unadjusted=False)

        df = _get_df_from_response(symbol, resp, dtype=dtype)
        if df is not None:
          dfs.append(df)

    df =  pd.concat(dfs, ignore_index=True) if dfs else None
    if df is not None:
      df = ut.purge_fetched_data(df, start_date, end_date, data_step)

    alog.debug0(f'Fetched {len(df) if df is not None else 0} records')

    return df

