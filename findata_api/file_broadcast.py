import array
import collections
import threading

import numpy as np
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu

from . import api_types
from . import stream_handlers as sth
from . import utils as ut


_TRADES = 1
_QUOTES = 2
_BARS = 3

_FIELDS_MAP = {
  _TRADES: api_types.StreamTrade,
  _QUOTES: api_types.StreamQuote,
  _BARS: api_types.StreamBar,
}

_LoadedFile = collections.namedtuple('LoadedFile', 'path, kind, cdata, ts_col',
                                     defaults=('timestamp',))


def _build_ntuple(nt, cdata, idx):
  nt_data = [cdata[n][idx] for n in nt._fields]

  return nt(*nt_data)


def _load_files(files, dtype):
  loaded_files = []

  for path in files:
    alog.debug0(f'Loading file: {path}')
    cdata = ut.npdict_dataframe(path, dtype=dtype, no_convert={'timestamp'})

    cfields, ckind = set(cdata.keys()), None
    for kind, nt in _FIELDS_MAP.items():
      kfields = set(nt._fields)
      rem_fields = cfields - kfields
      if not rem_fields:
        ckind = kind
        break

    if ckind is not None:
      loaded_files.append(_LoadedFile(path=path, kind=ckind, cdata=cdata))

  return tuple(loaded_files)


class FileBroadcast(sth.StreamHandlers):

  def __init__(self, files, dtype=np.float32):
    super().__init__()
    self._files = _load_files(pyu.as_sequence(files), dtype=dtype)
    self._thread = None

  def start(self):
    with self._lock:
      tas.check_is_none(self._thread, msg=f'File broadcast already started')
      self._thread = threading.Thread(target=self._run, name='FileBroadcast')
      self._thread.start()

  def stop(self):
    with self._lock:
      tas.check_is_not_none(self._thread, msg=f'File broadcast has not been started')
      thread = self._thread

    alog.debug0(f'Waiting for file broadcast to complete')
    thread.join()

    with self._lock:
      self._thread = None

  def _trade_handler(self, x):
    self._run_handlers(self.TRADE, x)

  def _quote_handler(self, x):
    self._run_handlers(self.QUOTE, x)

  def _bar_handler(self, x):
    self._run_handlers(self.BAR, x)

  def _run(self):
    times = array.array('d')
    fidx = array.array('L')
    ridx = array.array('L')
    for i, lfile in enumerate(self._files):
      ts_data = lfile.cdata[lfile.ts_col]
      times.extend(ts_data)
      fidx.extend(np.full(len(ts_data), i, dtype=np.int64))
      ridx.extend(np.arange(len(ts_data)))

    handlers = {
      _TRADES: self._trade_handler,
      _QUOTES: self._quote_handler,
      _BARS: self._bar_handler,
    }

    for i in np.argsort(times):
      lfile = self._files[fidx[i]]
      nt = _build_ntuple(_FIELDS_MAP[lfile.kind], lfile.cdata, ridx[i])

      handlers[lfile.kind](nt)

