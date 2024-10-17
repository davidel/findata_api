import collections
import threading

import numpy as np
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.utils as pyu

from . import data_source_base as dsb
from . import stream_computer as sc
from . import utils as ut


class StreamDataSource(dsb.DataSourceBase):

  def __init__(self, sbcast, symbols, data_step, scheduler=None, fetch_delay=15,
               fetch_time_depth=3600, dtype=np.float32):
    super().__init__(symbols, data_step,
                     scheduler=scheduler,
                     fetch_delay=fetch_delay,
                     fetch_time_depth=fetch_time_depth,
                     dtype=dtype)
    self._sbcast = sbcast
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

  def _try_poll(self):
    # The 'end_ts' calculation makes sure (since the get_trade_bars() API uses
    # a <end_ts selection constraint) that we do not pull the current bar,
    # which is still partially filled.
    now = self._scheduler.timegen.now()
    start_ts = now - self._fetch_time_depth
    end_ts = (now // self._step_delta) * self._step_delta

    bars = self._scomp.get_trade_bars(start_ts=start_ts, end_ts=end_ts)

    dfs = dict()
    for sym, sbars in bars.items():
      df = ut.make_bars_dataframe(sbars, dtype=self._dtype)
      df = df.sort_values('t', ignore_index=True)
      dfs[sym] = ut.transform_raw_data(df)

    self._run_bar_functions(dfs)

