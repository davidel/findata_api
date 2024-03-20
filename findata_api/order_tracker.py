import collections
import threading
import weakref

from py_misc_utils import abs_timeout as pyat
from py_misc_utils import alog
from py_misc_utils import scheduler as sch
from py_misc_utils import utils as pyu


PendingOrder = collections.namedtuple('PendingOrder', 'order_id, completed_fn, event')


def _wrap_complete_fn(executor, fn, *args, **kwargs):
  exref = weakref.ref(executor)

  def wrapped():
    try:
      return fn(*args, **kwargs)
    finally:
      xtor = exref()
      if xtor is not None:
        xtor._task_completed()

  return wrapped


class OrderTracker:

  def __init__(self, api, scheduler=None, refresh_time=None):
    self.api = api
    self.scheduler = scheduler or sch.common_scheduler()
    self._refresh_time = refresh_time or pyu.getenv('TRACKER_REFRESH', dtype=int, defval=5)
    self._lock = threading.Lock()
    self._pending_cv = threading.Condition(self._lock)
    self._orders = dict()
    self._in_flight = 0
    self._in_flight_cv = threading.Condition(self._lock)

  def clear(self):
    with self._lock:
      pending_orders = list(self._orders.items())
      self._orders.clear()
      self._pending_cv.notify_all()

    for order_id, pending_order in pending_orders:
      alog.debug0(f'Abandoning tracking order #{order_id}')
      self.scheduler.cancel(pending_order.event)

  def _is_completed(self, order):
    return order.status in {'filled', 'truncated'}

  def _task_completed(self):
    with self._lock:
      self._in_flight -= 1
      if self._in_flight == 0:
        self._in_flight_cv.notify_all()

  def _run_completed(self, completed_fn, order):
    wfn = _wrap_complete_fn(self, completed_fn, order)
    self.scheduler.executor.submit(wfn)
    self._in_flight += 1

  def _track_order(self, order_id):
    order = self.api.get_order(order_id)
    with self._lock:
      completed_order = None
      if self._is_completed(order):
        completed_order = self._orders.pop(order_id, None)
        if completed_order is not None:
          self._pending_cv.notify_all()
      elif order_id in self._orders:
        event = self.scheduler.enter(self._refresh_time, self._track_order,
                                     argument=(order_id,))
        self._orders[order_id] = pyu.new_with(self._orders[order_id],
                                              event=event)

      if completed_order is not None:
        self._run_completed(completed_order.completed_fn, order)

  def submit(self, completed_fn, *args, **kwargs):
    order = self.api.submit_order(*args, **kwargs)
    with self._lock:
      if self._is_completed(order):
        self._run_completed(completed_fn, order)
      else:
        event = self.scheduler.enter(self._refresh_time, self._track_order,
                                     argument=(order.id,))

        self._orders[order.id] = PendingOrder(order_id=order.id,
                                              completed_fn=completed_fn,
                                              event=event)

    return order

  def cancel(self, order_id):
    with self._lock:
      canceled_order = self._orders.pop(order_id, None)
      if canceled_order is not None:
        self._pending_cv.notify_all()

    if canceled_order is not None:
      self.scheduler.cancel(canceled_order.event)

      order = self.api.get_order(order_id)
      if self._is_completed(order):
        self.scheduler.executor.submit(canceled_order.completed_fn, order)
      else:
        self.api.cancel_order(order_id)
    else:
      self.api.cancel_order(order_id)

  def wait(self, order_id, timeout=None):
    timegen = self.scheduler.timegen
    atimeo = pyat.AbsTimeout(timeout, timefn=timegen.now)
    with self._lock:
      completed = True
      while order_id in self._orders:
        wait_time = atimeo.get()
        if wait_time is None or wait_time > 0:
          timegen.wait(self._pending_cv, timeout=wait_time)
        else:
          completed = False

      return completed

  def wait_all(self, timeout=None):
    timegen = self.scheduler.timegen
    atimeo = pyat.AbsTimeout(timeout, timefn=timegen.now)
    with self._lock:
      flushed = True
      while self._orders and flushed:
        wait_time = atimeo.get()
        if wait_time is None or wait_time > 0:
          timegen.wait(self._pending_cv, timeout=wait_time)
        else:
          flushed = False

      return flushed

  def pending(self):
    with self._lock:
      return tuple(self._orders.keys())

  def pending_orders(self):
    pending = self.pending()

    return tuple(self.api.get_order(oid) for oid in pending)

  def wait_in_flight(self, timeout=None):
    timegen = self.scheduler.timegen
    atimeo = pyat.AbsTimeout(timeout, timefn=timegen.now)
    with self._lock:
      while self._in_flight > 0:
        timegen.wait(self._in_flight_cv, timeout=atimeo.get())

      return self._in_flight

  def __len__(self):
    with self._lock:
      return len(self._orders)

