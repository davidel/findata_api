import collections
import threading

import py_misc_utils.abs_timeout as pyat
import py_misc_utils.alog as alog
import py_misc_utils.cond_waiter as pycw
import py_misc_utils.core_utils as pycu
import py_misc_utils.scheduler as sch
import py_misc_utils.utils as pyu


PendingOrder = collections.namedtuple('PendingOrder', 'order_id, completed_fn, event')


class OrderTracker:

  def __init__(self, api, scheduler=None, refresh_time=None):
    self.api = api
    self.scheduler = scheduler or sch.common_scheduler()
    self._refresh_time = refresh_time or pyu.getenv('TRACKER_REFRESH', dtype=int, defval=5)
    self._lock = threading.Lock()
    self._pending_cv = threading.Condition(self._lock)
    self._orders = dict()

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

  def _track_order(self, order_id):
    order, completed_order = self.api.get_order(order_id), None
    with self._lock:
      if self._is_completed(order):
        completed_order = self._orders.pop(order_id, None)
        if completed_order is not None:
          self._pending_cv.notify_all()
      elif order_id in self._orders:
        event = self.scheduler.enter(self._refresh_time, self._track_order,
                                     argument=(order_id,))
        self._orders[order_id] = pycu.new_with(self._orders[order_id],
                                               event=event)

    if completed_order is not None:
      self.scheduler.executor.submit(completed_order.completed_fn, order)

  def submit(self, completed_fn, *args, **kwargs):
    order = self.api.submit_order(*args, **kwargs)
    if self._is_completed(order):
      self.scheduler.executor.submit(completed_fn, order)
    else:
      with self._lock:
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
    waiter = pycw.CondWaiter(timeout=timeout, timegen=self.scheduler.timegen)
    with self._lock:
      completed = True
      while order_id in self._orders and completed:
        completed = waiter.wait(self._pending_cv)

      return completed

  def wait_all(self, timeout=None):
    waiter = pycw.CondWaiter(timeout=timeout, timegen=self.scheduler.timegen)
    with self._lock:
      flushed = True
      while self._orders and flushed:
        flushed = waiter.wait(self._pending_cv)

      return flushed

  def pending(self):
    with self._lock:
      return tuple(self._orders.keys())

  def pending_orders(self):
    pending = self.pending()

    return tuple(self.api.get_order(oid) for oid in pending)

  def __len__(self):
    with self._lock:
      return len(self._orders)

