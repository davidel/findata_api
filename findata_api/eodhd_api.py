import datetime
import io
import os
import requests
import socket
import threading
import websocket

import numpy as np
import orjson
import pandas as pd
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.context_base as pycb
import py_misc_utils.core_utils as pycu
import py_misc_utils.date_utils as pyd
import py_misc_utils.fin_wrap as pyfw
import py_misc_utils.no_except as pynex
import py_misc_utils.throttle as throttle
import py_misc_utils.utils as pyu

from . import api_base
from . import api_types
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

  period = _EOD_STEPS.get(mstep)
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

  def __init__(self, url, auth_key, process_message,
               on_open=None,
               on_close=None,
               on_error=None):
    self._url = f'{url}?api_token={auth_key}'
    self._run_thread = None
    self._ws = websocket.WebSocketApp(self._url,
                                      on_open=on_open,
                                      on_close=on_close,
                                      on_error=on_error,
                                      on_message=process_message)

  def run(self, **kwargs):
    self._ws.run_forever(**kwargs)

  def run_async(self, **kwargs):
    self._run_thread = pycu.run_async(self.run, **kwargs)

  def close_connection(self):
    self._ws.close()
    if self._run_thread is not None:
      self._run_thread.join()

  def subscribe(self, *symbols):
    symlist = ','.join(symbols)
    self._ws.send(f'{{"action":"subscribe","symbols":"{symlist}"}}')

  def unsubscribe(self, *symbols):
    symlist = ','.join(symbols)
    self._ws.send(f'{{"action":"unsubscribe","symbols":"{symlist}"}}')


class Stream(pycb.ContextBase):

  DEFAULT_CTX = dict(
    handler=None,
    started=False,
    ws_symbols=(),
  )

  SOCKET_BUFFER_SIZE = 8 * 1024 * 1024

  def __init__(self, api_key, name, url, marshal):
    super().__init__(self.DEFAULT_CTX)
    self._name = name
    self._marshal = marshal
    self._lock = threading.Lock()
    self._connected = threading.Event()
    self._ws_api = WebSocketClient(url, api_key,
                                   self._process_message,
                                   on_open=self._on_open,
                                   on_close=self._on_close,
                                   on_error=self._on_error)

  def _on_open(self, wsa):
    alog.info(f'[{self._name}] EODHD WebSocket connected')
    self._connected.set()

  def _reconnect(self):
    with self._lock:
      alog.info(f'[{self._name}] Reconnecting EODHD WebSocket')

      self._stop()
      self._start()

      ctx = self._ctx
      if ctx.ws_symbols:
        # Prevent _register() from unregistering existing symbols, as the connection
        # is not the same they were registered into.
        self._new_ctx(ws_symbols=())
        self._register(ctx.ws_symbols, ctx.handler)

  def _start(self):
    ctx = self._ctx
    if not ctx.started:
      alog.debug2(f'[{self._name}] Starting EODHD WebSocket connection')

      # We bump up the WebSocket buffer size to avoid message traffic peaks to
      # cause back pressure on the server side, which will result in the server
      # dropping the connection.
      buffer_size = pyu.getenv('WEBSOCK_BUFSIZE', dtype=int,
                               defval=Stream.SOCKET_BUFFER_SIZE)
      alog.debug0(f'[{self._name}] Using WebSocket receive buffer of {buffer_size} bytes')

      socket_options = (
        (socket.SOL_SOCKET, socket.SO_RCVBUF, buffer_size),
      )

      self._ws_api.run_async(sockopt=socket_options)
      self._new_ctx(started=True)

      alog.debug0(f'[{self._name}] Waiting for EODHD WebSocket connection ...')
      self._connected.wait()

  def start(self):
    with self._lock:
      self._start()

  def _stop(self):
    ctx = self._ctx
    if ctx.started:
      alog.debug2(f'[{self._name}] Stopping EODHD WebSocket connection')
      self._new_ctx(started=False)
      self._ws_api.close_connection()
      self._connected.clear()

  def stop(self):
    with self._lock:
      self._stop()

  def _on_close(self, wsa, status, msg):
    ctx = self._ctx
    alog.warning(f'[{self._name}] Streaming connection closed: ({status}) {msg}')
    if ctx.started:
      # Cannot do the reconnect work from inside the WebSocket callback. Just spawn
      # an async thread and do it from there. The WebSocket API will call the on-close
      # callback one all the teardown is completed, making it safe to issue a new
      # run_async().
      pycu.run_async(pynex.xwrap_fn(self._reconnect))

  def _on_error(self, wsa, error):
    alog.error(f'[{self._name}] Streaming connection error: {error}')

  def _process_message(self, wsa, msg):
    # Code within WebSocket callbacks should not take the lock. All the data needed
    # by the callback is stored within the context, whose content is never modified
    # (it gets just replaced with a new copy).
    ctx = self._ctx
    if ctx.handler is not None:
      data = orjson.loads(msg)
      mdata = self._marshal(data)
      if mdata is not None:
        ctx.handler(mdata)
      else:
        alog.info(f'[{self._name}] Stream Message: {data}')

  def _register(self, symbols, handler):
    ctx = self._ctx
    if ctx.ws_symbols:
      self._ws_api.unsubscribe(*ctx.ws_symbols)

    if symbols:
      self._ws_api.subscribe(*symbols)

    self._new_ctx(ws_symbols=tuple(symbols), handler=handler)

  def register(self, symbols, handler):
    with self._lock:
      self._register(symbols, handler)


def _get_stream_ts(v):
  return v / 1000


def _marshal_stream_trade(t):
  ts = t.get('t')
  if ts is not None:
    return api_types.StreamTrade(timestamp=_get_stream_ts(ts),
                                 symbol=t['s'],
                                 quantity=t['v'],
                                 price=t['p'])


def _marshal_stream_quote(q):
  ts = q.get('t')
  if ts is not None:
    return api_types.StreamQuote(timestamp=_get_stream_ts(ts),
                                 symbol=q['s'],
                                 bid_size=q['bs'],
                                 bid_price=q['bp'],
                                 ask_size=q['as'],
                                 ask_price=q['ap'])

class MultiStream:

  STREAM_PARAMS = {
    'trades': ('trades', 'wss://ws.eodhistoricaldata.com/ws/us', _marshal_stream_trade),
    'quotes': ('quotes', 'wss://ws.eodhistoricaldata.com/ws/us-quote', _marshal_stream_quote),
  }

  def __init__(self, api_key):
    self._api_key = api_key
    self._lock = threading.Lock()
    self._started = False
    self._streams = dict()

  def _ensure_stream(self, source):
    stream = self._streams.get(source)
    if stream is None:
      sparams = self.STREAM_PARAMS.get(source)
      if sparams is not None:
        stream = Stream(self._api_key, *sparams)

        if self._started:
          stream.start()

        self._streams[source] = stream

    return stream

  def _start(self):
    if not self._started:
      for stream in self._streams.values():
        stream.start()

      self._started = True

  def start(self):
    with self._lock:
      self._start()

  def _stop(self):
    if self._started:
      for stream in self._streams.values():
        stream.stop()

      self._started = False

  def stop(self):
    with self._lock:
      self._stop()

  def register(self, symbols, handlers):
    with self._lock:
      if not symbols:
        if self._started:
          for stream in self._streams.values():
            stream.stop()

        self._streams.clear()
      else:
        for source, handler in handlers.items():
          stream = self._ensure_stream(source)
          if stream is not None:
            stream.register(symbols, handler)
          else:
            alog.warning(f'Not supported by the EODHD streaming API: {source}')

        for source, stream in tuple(self._streams.items()):
          if source not in handlers:
            if self._started:
              stream.stop()
            self._streams.pop(source)


class API(api_base.API):
  # https://eodhd.com/financial-apis/intraday-historical-data-api
  # https://eodhd.com/financial-apis/api-for-historical-data-and-volumes

  def __init__(self, api_key=None, api_rate=None):
    super().__init__(name='EODHD', supports_streaming=True)
    self._api_key = api_key or pyu.getenv('EODHD_KEY')
    self._api_throttle = throttle.Throttle(
      (5 if api_rate is None else api_rate) / 60.0)
    self._stream = None

  def register_stream_handlers(self, symbols, handlers):
    if self._stream is None and symbols:
      stream = MultiStream(self._api_key)
      pyfw.fin_wrap(self, '_stream', stream, finfn=stream.stop)
      self._stream.start()

    alog.debug1(f'Registering Streaming: handlers={tuple(handlers.keys())}\tsymbols={symbols}')

    self._stream.register(symbols, handlers)

    alog.debug1(f'Registration done!')

    if self._stream is not None and not symbols:
      pyfw.fin_wrap(self, '_stream', None)

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

