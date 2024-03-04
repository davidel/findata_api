import os
import time

import pandas as pd
from py_misc_utils import alog
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu

from . import api_types


TRADES_FILE = 'trades'
QUOTES_FILE = 'quotes'
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
_TRADES_KIND = 1
_QUOTES_KIND = 2

_FORMATTER_FN = '_formatter'


def _open_logfile(path, header):
  lf = open(path, mode='at')
  if lf.tell() == 0:
    lf.write(f'{header}\n')

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


def trades_file(fmt=None):
  return f'{TRADES_FILE}.{fmt}' if fmt is not None else TRADES_FILE


def quotes_file(fmt=None):
  return f'{QUOTES_FILE}.{fmt}' if fmt is not None else QUOTES_FILE


def create_logging_handlers(path, stream_trades=True, stream_quotes=True,
                            logit=False):
  trade_handler = None
  if stream_trades:
    trades_path = os.path.join(path, trades_file(fmt='csv'))
    trades_cols = tuple([c[0] for c in _TRADES_COLS])
    trade_handler = _log_function(
      _open_logfile(trades_path, ','.join(trades_cols)),
      'TRADE',
      trades_cols,
      logit)
    alog.debug1(f'Saving stream TRADES to {trades_path}')

  quote_handler = None
  if stream_quotes:
    quotes_path = os.path.join(path, quotes_file(fmt='csv'))
    quotes_cols = tuple([c[0] for c in _QUOTES_COLS])
    quote_handler = _log_function(
      _open_logfile(quotes_path, ','.join(quotes_cols)),
      'QUOTE',
      quotes_cols,
      logit)
    alog.debug1(f'Saving stream QUOTES to {quotes_path}')

  return pyu.make_object(trade_handler=trade_handler, quote_handler=quote_handler)


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


class FileStream:

  def __init__(self, trades_path=None, quotes_path=None, trade_handler=None,
               quote_handler=None):
    self._trades_path = trades_path
    self._quotes_path = quotes_path
    self._trade_handler = trade_handler
    self._quote_handler = quote_handler

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

    alog.debug0(f'Sorting by timestamp {len(recs)} records ...')
    recs.sort(key=lambda r: r[0])
    alog.debug0(f'Sorting by timestamp {len(recs)} records ... done!')

    for r in recs:
      if r[1] == _TRADES_KIND:
        trade_handler(r)
      elif r[1] == _QUOTES_KIND:
        quote_handler(r)


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

  def __init__(self, trades_path=None, quotes_path=None, trade_handler=None,
               quote_handler=None, fmap=None, index='timestamp'):
    self._trades_path = trades_path
    self._quotes_path = quotes_path
    self._trade_handler = trade_handler
    self._quote_handler = quote_handler
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

    alog.debug0(f'Sorting by timestamp {len(recs)} records ...')
    recs.sort(key=lambda r: r[0])
    alog.debug0(f'Sorting by timestamp {len(recs)} records ... done!')

    for r in recs:
      if r[1] == _TRADES_KIND:
        trade_handler(r[2])
      elif r[1] == _QUOTES_KIND:
        quote_handler(r[2])


_STREAMERS = (
  pyu.make_object(ext='csv', stype=FileStream),
  pyu.make_object(ext='pkl', stype=DataFrameStream),
)

def create_streamer(path, trade_handler=None, quote_handler=None):
  if os.path.isdir(path):
    for sr in _STREAMERS:
      trades_path = os.path.join(path, trades_file(fmt=sr.ext))
      has_trades = os.path.exists(trades_path)
      quotes_path = os.path.join(path, quotes_file(fmt=sr.ext))
      has_quotes = os.path.exists(quotes_path)
      if ((trade_handler and has_trades) or
          (quote_handler and has_quotes)):
        if has_trades:
          alog.info(f'Streaming in trades from {trades_path}')
        if has_quotes:
          alog.info(f'Streaming in quotes from {quotes_path}')

        return sr.stype(trades_path=trades_path, quotes_path=quotes_path,
                        trade_handler=trade_handler, quote_handler=quote_handler)
  elif os.path.exists(path):
    _, ext = os.path.splitext(os.path.basename(path))
    for sr in _STREAMERS:
      if sr.ext == ext[1:]:
        alog.info(f'Streaming in trades from {path}')

        trades_path, quotes_path = None, None
        kind = _infer_stream_kind(path)
        if kind == _TRADES_KIND:
          trades_path = path
        elif kind == _QUOTES_KIND:
          quotes_path = path

        return sr.stype(trades_path=trades_path, quotes_path=quotes_path,
                        trade_handler=trade_handler, quote_handler=quote_handler)

  alog.xraise(RuntimeError, f'Unable to create streamer at {path}')

