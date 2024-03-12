import collections
import threading

from py_misc_utils import alog
from py_misc_utils import scheduler as sch
from py_misc_utils import utils as pyu


PendingOrder = collections.namedtuple('PendingOrder', 'order_id, completed_fn, event')


class OrderTracker:

  def __init__(self, api, scheduler=None, refresh_time=5):
    self._api = api
    self._scheduler = scheduler or sch.common_scheduler()
    self._refresh_time = refresh_time
    self._lock = threading.Lock()
    self._pending = threading.Condition(self._lock)
    self._orders = dict()

  def clear(self):
    with self._lock:
      pending_orders = list(self._orders.items())
      self._orders.clear()
      self._pending.notify_all()

    for order_id, pending_order in pending_orders:
      alog.debug0(f'Abandoning tracking order #{order_id}')
      self._scheduler.cancel(pending_order.event)

  def _is_completed(self, order):
    return order.status == 'filled'

  def _track_order(self, order_id):
    order = self._api.get_order(order_id)
    completed_order = None
    with self._lock:
      if self._is_completed(order):
        completed_order = self._orders.pop(order_id, None)
        if completed_order is not None:
          self._pending.notify_all()
      elif order_id in self._orders:
        event = self._scheduler.enter(self._refresh_time, self._track_order,
                                      argument=(order_id,))
        self._orders[order_id] = pyu.new_with(self._orders[order_id],
                                              event=event)

    if completed_order is not None:
      completed_order.completed_fn(order)

  def submit(self, completed_fn, *args, **kwargs):
    order = self._api.submit_order(*args, **kwargs)
    if self._is_completed(order):
      completed_fn(order)
    else:
      with self._lock:
        event = self._scheduler.enter(self._refresh_time, self._track_order,
                                      argument=(order.id,))

        self._orders[order.id] = PendingOrder(order_id=order.id,
                                              completed_fn=completed_fn,
                                              event=event)

    return order

  def cancel(self, order_id):
    with self._lock:
      canceled_order = self._orders.pop(order_id, None)
      if canceled_order is not None:
        self._pending.notify_all()

    if canceled_order is not None:
      self._scheduler.cancel(canceled_order.event)

      order = self._api.get_order(order_id)
      if self._is_completed(order):
        canceled_order.completed_fn(order)
      else:
        self._api.cancel_order(order_id)
    else:
      self._api.cancel_order(order_id)

  def wait(self, timeout=None):
    timegen = self._scheduler.timegen
    exit_time = timegen.now() + timeout if timeout is not None else None
    with self._lock:
      flushed = True
      while self._orders and flushed:
        wait_time = exit_time - timegen.now() if timeout is not None else None
        if wait_time is None or wait_time > 0:
          timegen.wait(self._pending, timeout=wait_time)
        else:
          flushed = False

      return flushed

  def pending(self):
    with self._lock:
      return tuple(self._orders.keys())

  def pending_orders(self):
    pending = self.pending()

    return tuple(self._api.get_order(oid) for oid in pending)

