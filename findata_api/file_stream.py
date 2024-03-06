import os
import time

from py_misc_utils import alog
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu

from . import api_types


TRADES_FILE = 'trades'
QUOTES_FILE = 'quotes'
BARS_FILE = 'bars'
_TRADES_COLS = (
  ('timestamp', float),
  ('symbol', str),
  ('quantity', float),
  ('price', float),
)
_QUOTES_COLS = (
  ('timestamp', float),
  ('symbol', str),
  ('bid_size', float),
  ('bid_price', float),
  ('ask_size', float),
  ('ask_price', float),
)
_BARS_COLS = (
  ('timestamp', float),
  ('symbol', str),
  ('open', float),
  ('high', float),
  ('low', float),
  ('close', float),
  ('volume', float),
)
_TRADES_KIND = 1
_QUOTES_KIND = 2
_BARS_KIND = 3

_FORMATTER_FN = '_formatter'


def _open_logfile(path, header):
  lf = open(path, mode='at')
  if lf.tell() == 0:
    lf.write(f'{header}\n')
    lf.flush()

  return lf


def _csv_formatter(name, cols):
  xcols = [f'{{x.{n}}}' for n in cols]

  return f"def {name}(x):\n  return f'" + ','.join(xcols) + "'\n"


def _compile_formatter(cols):
  env = dict()
  exec(_csv_formatter(_FORMATTER_FN, cols), env)

  return env[_FORMATTER_FN]


def _log_function(lf, kind, cols, logit):
  # Static format strings are much faster than format() or join(), but must be static.
  # So we generate a function which uses static format strings to generate CSV data.
  formatter = _compile_formatter(cols)

  def logfn(x):
    data = formatter(x)
    lf.write(data + '\n')
    if logit:
      alog.info(f'{kind} = {data}')

  return logfn


def create_logging_handlers(path, stream_trades=True, stream_quotes=True,
                            stream_bars=True, logit=False):
  trade_handler = None
  if stream_trades:
    trades_path = os.path.join(path, f'{TRADES_FILE}.csv')
    trades_cols = tuple([c[0] for c in _TRADES_COLS])
    trade_handler = _log_function(
      _open_logfile(trades_path, ','.join(trades_cols)),
      'TRADE',
      trades_cols,
      logit)
    alog.debug1(f'Saving stream TRADES to {trades_path}')

  quote_handler = None
  if stream_quotes:
    quotes_path = os.path.join(path, f'{QUOTES_FILE}.csv')
    quotes_cols = tuple([c[0] for c in _QUOTES_COLS])
    quote_handler = _log_function(
      _open_logfile(quotes_path, ','.join(quotes_cols)),
      'QUOTE',
      quotes_cols,
      logit)
    alog.debug1(f'Saving stream QUOTES to {quotes_path}')

  bar_handler = None
  if stream_bars:
    bars_path = os.path.join(path, f'{BARS_FILE}.csv')
    bars_cols = tuple([c[0] for c in _BARS_COLS])
    bar_handler = _log_function(
      _open_logfile(bars_path, ','.join(bars_cols)),
      'BAR',
      bars_cols,
      logit)
    alog.debug1(f'Saving stream BARS to {bars_path}')

  return pyu.make_object(trade_handler=trade_handler,
                         quote_handler=quote_handler,
                         bar_handler=bar_handler)


def _get_csv_args(parts, cols, indices):
  args = dict()
  for i, (c, ctype) in enumerate(cols):
    args[c] = ctype(parts[indices[i]])

  return args


def _read_lines(path):
  with open(path, mode='r') as f:
    return f.read().splitlines()


def _read_data(path, kind):
  lines = _read_lines(path)

  cols = lines[0].split(',')
  cidx = pyu.make_index_dict(cols)
  ti = cidx['timestamp']
  recs = []
  for ln in lines[1:]:
    parts = ln.split(',')
    recs.append((float(parts[ti]), kind, parts))

  return recs, cols, cidx


def _make_csv_handler(path, kind, scols, shandler, stype):
  alog.debug0(f'Reading {path} ...')
  recs, cols, cidx = _read_data(path, kind)
  alog.debug0(f'Read {len(recs)} records from {path}')

  indices = tuple(cidx[c[0]] for c in scols)

  def handler(r):
    args = _get_csv_args(r[2], scols, indices)
    shandler(stype(**args))

  return recs, handler


def _infer_stream_kind(path):
  with open(path, mode='r') as f:
    cols = f.readline().rstrip('\r\n').split(',')

  scols = set(cols)
  if scols == set(c[0] for c in _TRADES_COLS):
    return _TRADES_KIND
  if scols == set(c[0] for c in _QUOTES_COLS):
    return _QUOTES_KIND
  if scols == set(c[0] for c in _BARS_COLS):
    return _BARS_KIND


class FileStream:

  def __init__(self, trades_path=None, quotes_path=None, bars_path=None,
               trade_handler=None, quote_handler=None, bar_handler=None):
    self._trades_path = trades_path
    self._quotes_path = quotes_path
    self._trade_handler = trade_handler
    self._quote_handler = quote_handler
    self._bar_handler = bar_handler

  def run(self):
    recs = []

    if self._trade_handler and os.path.exists(self._trades_path):
      trade_recs, trade_handler = _make_csv_handler(
        self._trades_path,
        _TRADES_KIND,
        _TRADES_COLS,
        self._trade_handler,
        api_types.StreamTrade)
      recs += trade_recs

    if self._quote_handler and os.path.exists(self._quotes_path):
      quote_recs, quote_handler = _make_csv_handler(
        self._quotes_path,
        _QUOTES_KIND,
        _QUOTES_COLS,
        self._quote_handler,
        api_types.StreamQuote)
      recs += quote_recs

    if self._bar_handler and os.path.exists(self._bars_path):
      bar_recs, bar_handler = _make_csv_handler(
        self._bars_path,
        _BARS_KIND,
        _BARS_COLS,
        self._bar_handler,
        api_types.StreamBar)
      recs += bar_recs

    alog.debug0(f'Sorting by timestamp {len(recs)} records ...')
    recs.sort(key=lambda r: r[0])
    alog.debug0(f'Sorting by timestamp {len(recs)} records ... done!')

    for r in recs:
      if r[1] == _TRADES_KIND:
        trade_handler(r)
      elif r[1] == _QUOTES_KIND:
        quote_handler(r)
      elif r[1] == _BARS_KIND:
        bar_handler(r)


def _make_pkl_handler(path, kind, scols, shandler, stype,
                      fmap=None, index='timestamp'):
  alog.debug0(f'Reading {path} ...')
  df = pyp.load_dataframe(path)
  alog.debug0(f'Read {len(df)} records from {path}')

  recs = [(t, kind, i) for i, t in enumerate(pyp.column_or_index(df, index))]

  data = dict()
  for c in scols:
    fn = fmap.get(c[0], c[0]) if fmap is not None else c[0]
    data[c[0]] = df[fn].to_numpy()

  def handler(i):
    args = dict()
    for n, nd in data.items():
      args[n] = nd[i]
    shandler(stype(**args))

  return recs, handler


class DataFrameStream:

  def __init__(self, trades_path=None, quotes_path=None, bars_path=None,
               trade_handler=None, quote_handler=None, bar_handler=None,
               fmap=None, index='timestamp'):
    self._trades_path = trades_path
    self._quotes_path = quotes_path
    self._bars_path = bars_path
    self._trade_handler = trade_handler
    self._quote_handler = quote_handler
    self._bar_handler = bar_handler
    self._fmap = fmap
    self._index = index

  def run(self):
    recs = []

    if self._trade_handler and os.path.exists(self._trades_path):
      trade_recs, trade_handler = _make_pkl_handler(
        self._trades_path,
        _TRADES_KIND,
        _TRADES_COLS,
        self._trade_handler,
        api_types.StreamTrade,
        fmap=self._fmap,
        index=self._index)
      recs += trade_recs

    if self._quote_handler and os.path.exists(self._quotes_path):
      quote_recs, quote_handler = _make_pkl_handler(
        self._quotes_path,
        _QUOTES_KIND,
        _QUOTES_COLS,
        self._quote_handler,
        api_types.StreamQuote,
        fmap=self._fmap,
        index=self._index)
      recs += quote_recs

    if self._bar_handler and os.path.exists(self._bars_path):
      bar_recs, bar_handler = _make_pkl_handler(
        self._bars_path,
        _BARS_KIND,
        _BARS_COLS,
        self._bar_handler,
        api_types.StreamBar,
        fmap=self._fmap,
        index=self._index)
      recs += bar_recs

    alog.debug0(f'Sorting by timestamp {len(recs)} records ...')
    recs.sort(key=lambda r: r[0])
    alog.debug0(f'Sorting by timestamp {len(recs)} records ... done!')

    for r in recs:
      if r[1] == _TRADES_KIND:
        trade_handler(r[2])
      elif r[1] == _QUOTES_KIND:
        quote_handler(r[2])
      elif r[1] == _BARS_KIND:
        bar_handler(r[2])


_STREAMERS = (
  pyu.make_object(ext='csv', stype=FileStream),
  pyu.make_object(ext='pkl', stype=DataFrameStream),
)

def create_streamer(path, trade_handler=None, quote_handler=None, bar_handler=None):
  if os.path.isdir(path):
    for sr in _STREAMERS:
      trades_path = os.path.join(path, f'{TRADES_FILE}.{sr.ext}')
      has_trades = os.path.exists(trades_path)

      quotes_path = os.path.join(path, f'{QUOTES_FILE}.{sr.ext}')
      has_quotes = os.path.exists(quotes_path)

      bars_path = os.path.join(path, f'{BARS_FILE}.{sr.ext}')
      has_bars = os.path.exists(bars_path)

      if ((trade_handler and has_trades) or
          (quote_handler and has_quotes) or
          (bar_handler and has_bars)):
        if has_trades:
          alog.info(f'Streaming in trades from {trades_path}')
        if has_quotes:
          alog.info(f'Streaming in quotes from {quotes_path}')
        if has_bars:
          alog.info(f'Streaming in bars from {bars_path}')

        return sr.stype(trades_path=trades_path,
                        quotes_path=quotes_path,
                        bars_path=bars_path,
                        trade_handler=trade_handler,
                        quote_handler=quote_handler,
                        bar_handler=bar_handler)
  elif os.path.exists(path):
    _, ext = os.path.splitext(os.path.basename(path))
    for sr in _STREAMERS:
      if sr.ext == ext[1:]:
        alog.info(f'Streaming in trades from {path}')

        trades_path, quotes_path, bars_path = None, None, None
        kind = _infer_stream_kind(path)
        if kind == _TRADES_KIND:
          trades_path = path
        elif kind == _QUOTES_KIND:
          quotes_path = path
        elif kind == _BARS_KIND:
          bars_path = path

        return sr.stype(trades_path=trades_path,
                        quotes_path=quotes_path,
                        bars_path=bars_path,
                        trade_handler=trade_handler,
                        quote_handler=quote_handler,
                        bar_handler=bar_handler)

  alog.xraise(RuntimeError, f'Unable to create streamer at {path}')

