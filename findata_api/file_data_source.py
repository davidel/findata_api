import collections
import time

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import np_utils as pyn
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu

from . import stream_data_base as sdb
from . import utils as ut


def _load_dataframe(path, dtype):
  df = pyp.load_dataframe(path, dtype=dtype)

  if df.index.name == 't':
    df = df.reset_index()
  else:
    tas.check('t' in df, msg=f'File source must have a timestamp column named "t"')

  return {c: df[c].to_numpy() for c in df.columns}


class FileDataSource(sdb.StreamDataBase):

  def __init__(self, path, scheduler=None, dtype=np.float32):
    super().__init__(scheduler=scheduler)
    self._next_ts = None
    self._cdata = _load_dataframe(path, dtype)

  def start(self):
    self._start()

  def stop(self):
    self._stop()
    self._next_ts = None

  def _next_poll_time(self):
    # Only return one schedule time, as all the file will be fed from the
    # _try_poll() API at once.
    if self._next_ts is None:
      self._next_ts = time.time()

      return self._next_ts

  def _try_poll(self):
    times = self._cdata['t']
    indices = np.argsort(times)
    stimes = times[indices]

    splits = np.append(pyn.group_splits(stimes, lambda x: x != 0) + 1, len(stimes))

    symbols = self._cdata.get('symbol', None)

    base = 0
    for st in splits:
      end = st
      tindices = indices[base: end]

      sym_data = collections.defaultdict(lambda: collections.defaultdict(list))

      if symbols is not None:
        for i in tindices:
          sym = symbols[i]
          for c, data in self._cdata.items():
            if c != 't':
              fsym, field = ut.split_field(c)
              sym_data[sym][field].append(data[i])
      else:
        for i in tindices:
          for c, data in self._cdata.items():
            if c != 't':
              sym, field = ut.split_field(c)
              sym_data[sym][field].append(data[i])

      base = end

      tsplit = times[tindices[0]] if tindices else None

      dfs = dict()
      for sym, fdata in sym_data.items():
        fdata['t'] = [tsplit] * len(tindices)
        dfs[sym] = pd.DataFrame(data=fdata)

      self._run_bar_functions(dfs)

