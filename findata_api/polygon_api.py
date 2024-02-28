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


def _get_config(key, enable_streaming):
  if key is None:
    key = pyu.getenv('POLYGON_API_KEY')
  if enable_streaming is None:
    enable_streaming = pyu.to_bool(pyu.env('POLYGON_STREAMING', False))

  alog.debug0(f'Polygon API created with: key={key}\tenable_streaming={enable_streaming}')

  return key, enable_streaming


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


def _sublist(symbols, trades, quotes):
  sub = []
  for sym in symbols:
    if trades:
      sub.append(f'T.{sym}')
    if quotes:
      sub.append(f'Q.{sym}')

  return sub


class Stream:

  DEFAULT_CTX = dict(
    trade_handler=None,
    quote_handler=None,
    started=False,
    ws_symbols=(),
  )

  SOCKET_BUFFER_SIZE = 8 * 1024 * 1024

  def __init__(self, api_key):
    self._api_key = api_key
    self._lock = threading.Lock()
    self._ctx = Stream._make_ctx()
    self._ws_api = polygon.WebSocketClient(polygon.STOCKS_CLUSTER, self._api_key,
                                           process_message=self._process_message,
                                           on_close=self._on_close,
                                           on_error=self._on_error)

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
        self._register(ctx.ws_symbols,
                       trade_handler=ctx.trade_handler,
                       quote_handler=ctx.quote_handler)

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
        if ctx.quote_handler is not None:
          ctx.quote_handler(_marshal_stream_quote(d))
      elif kind == 'T':
        if ctx.trade_handler is not None:
          ctx.trade_handler(_marshal_stream_trade(d))
      else:
        alog.debug0(f'Stream Message: {d}')

  def _register(self, symbols, trade_handler=None, quote_handler=None):
    ctx = self._ctx
    unsub = _sublist(ctx.ws_symbols, ctx.trade_handler, ctx.quote_handler)
    if unsub:
      self._ws_api.unsubscribe(*unsub)

    sub = _sublist(symbols, trade_handler, quote_handler)
    if sub:
      self._ws_api.subscribe(*sub)

    self._new_ctx(ws_symbols=tuple(symbols),
                  trade_handler=trade_handler,
                  quote_handler=quote_handler)

  def register(self, symbols, trade_handler=None, quote_handler=None):
    with self._lock:
      self._register(symbols, trade_handler=trade_handler, quote_handler=quote_handler)


class API(api_base.API):

  def __init__(self, api_key=None, api_rate=None, enable_streaming=None):
    super().__init__()
    self._api_key, self._enable_streaming = _get_config(api_key, enable_streaming)
    self._api = polygon.RESTClient(self._api_key)
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)
    if self._enable_streaming:
      self._stream = Stream(self._api_key)
      self._stream.start()
    else:
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

  def register_stream_handlers(self, symbols, trade_handler=None, quote_handler=None):
    tas.check_is_not_none(self._stream, msg=f'Streaming is not enabled for the Polygon API')

    alog.debug1(f'Registering Streaming: trades={trade_handler is not None}\tquotes={quote_handler is not None}\tsymbols={symbols}')
    self._stream.register(symbols, trade_handler=trade_handler, quote_handler=quote_handler)
    alog.debug1(f'Registration done!')

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

