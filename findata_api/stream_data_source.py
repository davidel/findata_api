import collections
import datetime
import threading
import time

import numpy as np
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import utils as pyu

from . import stream_computer as sc
from . import stream_data_base as sdb
from . import utils as ut


class StreamDataSource(sdb.StreamDataBase):

  def __init__(self, sbcast, symbols, data_step, scheduler=None, fetch_delay=15,
               fetch_time_depth=3600, dtype=np.float32):
    super().__init__(scheduler=scheduler)
    self._sbcast = sbcast
    self._symbols = sorted(symbols)
    self._step_delta = round(ut.get_data_step_delta(data_step).total_seconds())
    self._fetch_delay = fetch_delay
    self._fetch_time_depth = fetch_time_depth
    self._dtype = dtype
    self._scomp = sc.StreamComputer(bar_interval=self._step_delta, symbols=symbols)
    self._sbcast.add_trade_handler(self._scomp.trade_handler)
    self._sbcast.add_quote_handler(self._scomp.quote_handler)

  def start(self):
    started = self._start()

    if started == 0:
      alog.info('Data poller starting')
      self._sbcast.start()

  def stop(self):
    started = self._stop()

    if started == 1:
      self._sbcast.stop()

  def _next_poll_time(self):
    return pyu.round_up(time.time(), self._step_delta) + self._fetch_delay

  def _try_poll(self):
    # The 'end_ts' calculation makes sure (since the get_trade_bars() API uses
    # a <end_ts selection constraint) that we do not pull the current bar,
    # which is still partially filled.
    now = time.time()
    start_ts = now - self._fetch_time_depth
    end_ts = (now // self._step_delta) * self._step_delta

    bars = self._scomp.get_trade_bars(start_ts=start_ts, end_ts=end_ts)

    dfs = dict()
    for sym, sbars in bars.items():
      df = ut.make_bars_dataframe(sbars, dtype=self._dtype)
      df = df.sort_values('t', ignore_index=True)
      dfs[sym] = ut.transform_raw_data(df)

    self._run_bar_functions(dfs)

