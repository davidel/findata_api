import threading

from py_misc_utils import alog
from py_misc_utils import date_utils as pyd
from py_misc_utils import scheduler as sch
from py_misc_utils import utils as pyu


class StreamDataBase:

  def __init__(self, scheduler=None):
    self._scheduler = scheduler or sch.common_scheduler()
    self._lock = threading.Lock()
    self._poll_event = None
    self._bar_functions = ()
    self._started = 0

  def _start(self):
    with self._lock:
      started = self._started
      self._started += 1

    if started == 0:
      self._schedule_poll()

    return started

  def _stop(self):
    with self._lock:
      started = self._started
      self._started -= 1

      if started == 1 and self._poll_event is not None:
        alog.info('Data poller stopping')
        self._scheduler.cancel(self._poll_event)
        self._poll_event = None

    return started

  def register_bar_fn(self, bar_fn):
    with self._lock:
      if bar_fn in self._bar_functions:
        return False
      self._bar_functions += tuple([bar_fn])

      return True

  def unregister_bar_fn(self, bar_fn):
    with self._lock:
      bar_functions = list(self._bar_functions)
      removed = pyu.checked_remove(bar_functions, bar_fn)
      if removed:
        self._bar_functions = tuple(bar_functions)

      return removed

  def _get_bar_functions(self):
    with self._lock:
      return self._bar_functions

  def _poll_helper(self):
    try:
      self._try_poll()
    except Exception as e:
      alog.exception(e, exmsg=f'Exception while running poll function: {self._try_poll}')
    self._schedule_poll()

  def _schedule_poll(self):
    ts = self._next_poll_time()

    with self._lock:
      if self._started > 0 and ts is not None:
        alog.debug0(f'Scheduling next poll at {pyd.from_timestamp(ts)}')
        self._poll_event = self._scheduler.enterabs(ts, self._poll_helper)
      else:
        self._poll_event = None

  def _run_bar_functions(self, *args, **kwargs):
    for bar_fn in self._get_bar_functions():
      try:
        bar_fn(*args, **kwargs)
      except Exception as e:
        alog.exception(e, exmsg=f'Exception while running bar function: {bar_fn}')

