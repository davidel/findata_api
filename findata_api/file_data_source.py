import collections
import os
import threading

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import np_utils as pyn
from py_misc_utils import pd_utils as pyp
from py_misc_utils import stream_dataframe as stdf
from py_misc_utils import utils as pyu

from . import stream_data_base as sdb
from . import utils as ut


def _enumerate_dataframe(path, dtype, args):
  cdata = pyp.load_dataframe_as_npdict(path,
                                       reset_index=True,
                                       dtype=dtype,
                                       no_convert={'t'})

  times = cdata['t']
  indices = np.argsort(times)
  stimes = times[indices]

  splits = np.append(pyn.group_splits(stimes, lambda x: x != 0) + 1, len(stimes))

  symbols = cdata.get('symbol', None)

  base = 0
  for st in splits:
    end = st
    tindices = indices[base: end]

    sym_data = collections.defaultdict(lambda: collections.defaultdict(list))

    if symbols is not None:
      for c, data in cdata.items():
        if c != 't':
          fsym, field = ut.split_field(c)
          for i in tindices:
            sym = symbols[i]
            sym_data[sym][field].append(data[i])
    else:
      for c, data in cdata.items():
        if c != 't':
          sym, field = ut.split_field(c)
          symd = sym_data[sym]
          for i in tindices:
            symd[field].append(data[i])

    base = end

    tsplit = times[tindices[0]] if tindices.size > 0 else None

    dfs = dict()
    for sym, fdata in sym_data.items():
      fdata['t'] = [tsplit] * len(pyu.seqfirst(fdata.values()))
      dfs[sym] = pd.DataFrame(data=fdata)

    yield dfs


def _enumerate_stream_dataframe(path, dtype, args):
  alog.debug0(f'Loading stream DataFrame from {path}')
  reader = stdf.StreamDataReader(path)

  time_scan = stdf.StreamSortedScan(reader, 't',
                                    slice_size=args.get('slice_size', 10000))
  for size, rdata in time_scan.scan():
    symbol = rdata['symbol']


    times = rdata['t']
    ts = pyd.from_timestamp(times[0])
    te = pyd.from_timestamp(times[-1])
    alog.warning(f'++++++++++++++++ {ts} ... {te}  ({len(times)})')
    if np.max(times) > times[-1]:
      alog.xraise(f'WOOOOOT {pyd.from_timestamp(np.max(times))} > {te}')
    mn = len(times) // 10
    for n in range(0, len(times), mn):
      xx = {field: data[n] for field, data in rdata.items()}
      alog.warning(f'[{n}] ******** {xx}')



    sym_data = collections.defaultdict(lambda: collections.defaultdict(list))
    for i, sym in enumerate(symbol):
      symd = sym_data[sym]
      for field, data in rdata.items():
        symd[field].append(data[i])

    dfs = dict()
    for sym, sdata in sym_data.items():
      df = pd.DataFrame(data=sdata)

      if dtype is not None:
        if not isinstance(dtype, dict):
          ddtype = {c: dtype for c in ('o', 'h', 'l', 'c', 'v')}
          ddtype['t'] = np.int64
          dtype = ddtype

        df = pyp.type_convert_dataframe(df, dtype)

      dfs[sym] = df

    yield dfs


def _enumerate_symbars(path, dtype, args):
  if os.path.isfile(path):
    yield from _enumerate_dataframe(path, dtype, args)
  elif os.path.isdir(path):
    yield from _enumerate_stream_dataframe(path, dtype, args)
  else:
    alog.xraise(f'Missing or unrecognized file format: {path}')


class FileDataSource(sdb.StreamDataBase):

  def __init__(self, path, scheduler=None, dtype=None, **kwargs):
    super().__init__(scheduler=scheduler)
    self._path = path
    self._dtype = dtype
    self._kwargs = kwargs
    self._term = threading.Event()
    self._next_ts = None

  def start(self):
    self._start()

  def stop(self):
    if self._stop() == 1:
      alog.debug0(f'Waiting for file data source to complete')
      self._term.wait()

      self._next_ts = None
      self._term.clear()

  def _next_poll_time(self):
    # Only return one schedule time, as all the file will be fed from the
    # _try_poll() API at once.
    if self._next_ts is None:
      self._next_ts = self._scheduler.timegen.now()

      return self._next_ts

  def _feed_data(self):
    for dfs in _enumerate_symbars(self._path, self._dtype, self._kwargs):
      self._run_bar_functions(dfs)

  def _try_poll(self):
    try:
      self._feed_data()
    finally:
      self._term.set()

