import datetime
import time

import numpy as np
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import utils as pyu

from . import stream_data_base as sdb
from . import utils as ut


class PolledDataSource(sdb.StreamDataBase):

  def __init__(self, api, symbols, data_step, scheduler=None, fetch_delay=15,
               fetch_time_depth=3600, dtype=np.float32):
    super().__init__(scheduler=scheduler)
    self._api = api
    self._symbols = sorted(symbols)
    self._data_step = data_step
    self._step_delta = round(ut.get_data_step_delta(data_step).total_seconds())
    self._fetch_delay = fetch_delay
    self._fetch_time_depth = fetch_time_depth
    self._dtype = dtype

  def start(self):
    self._start()

  def stop(self):
    self._stop()

  def _next_poll_time(self):
    return pyu.round_up(time.time(), self._step_delta) + self._fetch_delay

  def _try_poll(self):
    now = pyd.now()
    start_date = now - datetime.timedelta(seconds=self._fetch_time_depth)

    dfs = ut.create_split_data(self._api, start_date, now, self._symbols,
                               data_step=self._data_step,
                               dtype=self._dtype)

    self._run_bar_functions(dfs)

