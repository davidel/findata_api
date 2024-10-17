import datetime
import time

import numpy as np
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.date_utils as pyd
import py_misc_utils.utils as pyu

from . import data_source_base as dsb
from . import utils as ut


class PolledDataSource(dsb.DataSourceBase):

  def __init__(self, api, symbols, data_step, scheduler=None, fetch_delay=15,
               fetch_time_depth=3600, dtype=np.float32):
    super().__init__(symbols, data_step,
                     scheduler=scheduler,
                     fetch_delay=fetch_delay,
                     fetch_time_depth=fetch_time_depth,
                     dtype=dtype)
    self._api = api

  def start(self):
    self._start()

  def stop(self):
    self._stop()

  def _try_poll(self):
    now = pyd.now()
    start_date = now - datetime.timedelta(seconds=self._fetch_time_depth)

    dfs = ut.create_split_data(self._api, start_date, now, self._symbols,
                               data_step=self._data_step,
                               dtype=self._dtype)

    self._run_bar_functions(dfs)

