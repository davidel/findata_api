import numpy as np
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.date_utils as pyd
import py_misc_utils.num_utils as pynu

from . import stream_data_base as sdb
from . import utils as ut


class DataSourceBase(sdb.StreamDataBase):

  def __init__(self, symbols, data_step, scheduler=None, fetch_delay=15,
               fetch_time_depth=3600, dtype=np.float32):
    super().__init__(scheduler=scheduler)
    self._symbols = sorted(symbols)
    self._data_step = data_step
    self._step_delta = round(ut.get_data_step_delta(data_step).total_seconds())
    self._fetch_delay = fetch_delay
    self._fetch_time_depth = fetch_time_depth
    self._dtype = dtype

  def _next_poll_time(self):
    return pynu.round_up(self._scheduler.timegen.now(), self._step_delta) + self._fetch_delay

