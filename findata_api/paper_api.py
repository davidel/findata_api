import collections
import datetime
import heapq
import os
import pickle
import threading

import numpy as np
import pandas as pd
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.core_utils as pycu
import py_misc_utils.date_utils as pyd
import py_misc_utils.executor as pyex
import py_misc_utils.key_wrap as pykw
import py_misc_utils.pd_utils as pyp
import py_misc_utils.scheduler as sch
import py_misc_utils.state as pyst
import py_misc_utils.timegen as pytg
import py_misc_utils.utils as pyu

from . import api_base
from . import api_types
from . import market_hours as mh
from . import utils as ut


MODULE_NAME = 'PAPER'

def add_api_options(parser):
  parser.add_argument('--paper_key',
                      type=str,
                      default=os.getenv('PAPER_KEY', 'paper-account'),
                      help='The Paper API key')
  parser.add_argument('--paper_capital',
                      type=float,
                      default=pyu.getenv('PAPER_CAPITAL', dtype=float, defval=100_000),
                      help='The initial amount available for trading (in case ' \
                      '--paper_path points to a valid configuration, this amount is ' \
                      'taken from there)')
  parser.add_argument('--paper_path', type=str, default=os.getcwd(),
                      help='The path where the Paper API stores its configuration')

def create_api(args):
  return API(args.paper_key, args.paper_capital, args.paper_path)


def _marshal_order(o):
  return api_types.Order(id=o.id,
                         symbol=o.symbol,
                         quantity=o.quantity,
                         side=o.side,
                         type=o.type,
                         limit=o.limit,
                         stop=o.stop,
                         status=o.status,
                         created=o.created,
                         filled=o.filled,
                         filled_quantity=o.filled_quantity,
                         filled_avg_price=o.filled_avg_price)


def _marshal_position(p):
  return api_types.Position(symbol=p.symbol,
                            quantity=p.quantity,
                            value=p.quantity * p.price)


Price = collections.namedtuple('Price', 'price, timestamp')
Order = collections.namedtuple(
  'Order',
  'id, symbol, quantity, side, type, created, filled, filled_avg_price, ' \
  'filled_quantity, status, limit, stop, ref_price',
  defaults=(0, 0, 'new', None, None, None))
Position = collections.namedtuple('Position', 'symbol, quantity, price, timestamp, order_id')


class TimeGen:

  Wait = pykw.key_wrap('Wait', 'wakeup_time')

  def __init__(self):
    self._lock = threading.Lock()
    self._time = 0
    self._waits = []

  def now(self):
    return self._time

  def wait(self, cond, timeout=None):
    if timeout is not None:
      if timeout <= 0:
        return False

      with self._lock:
        wakeup_time = self._time + timeout
        wait = self.Wait(wakeup_time, cond=cond, expired=False)
        heapq.heappush(self._waits, wait)
    else:
      wait = None

    cond.wait()

    if wait is None:
      return True

    with self._lock:
      return not wait.expired

  def set_time(self, current_time):
    wakes = []
    with self._lock:
      self._time = max(self._time, current_time)

      while self._waits and self._time >= self._waits[0].wakeup_time:
        wait = heapq.heappop(self._waits)
        wait.expired = True
        wakes.append(wait)

    for wait in wakes:
      with wait.cond:
        wait.cond.notify_all()


class API(api_base.TradeAPI):

  def __init__(self, api_key, capital, fill_pct=None, fill_delay=None,
               refresh_time=None, executor=None):
    executor = executor if executor is not None else pyex.common_executor()

    super().__init__(name='Paper',
                     scheduler=sch.Scheduler(timegen=TimeGen(), executor=executor),
                     refresh_time=refresh_time)

    self._api_key = api_key
    self._capital = capital
    self._fill_pct = fill_pct
    self._fill_delay = fill_delay or 1.0
    self._schedref = self.scheduler.gen_unique_ref()
    self._lock = threading.Lock()
    self._prices = dict()
    self._orders = dict()
    self._positions = collections.defaultdict(list)
    self._stops = collections.defaultdict(list)
    self._order_id = 1

  def _get_state(self, state):
    cstate = api_base.TradeAPI._get_state(self, state)
    cstate.pop('_lock')

    return cstate

  def _set_state(self, state):
    executor = state.pop('executor', None)
    executor = executor if executor is not None else pyex.common_executor()

    state['scheduler'] = sch.Scheduler(timegen=TimeGen(), executor=executor)

    api_base.TradeAPI._set_state(self, state)
    self._lock = threading.Lock()

  def close(self, path=None):
    self.scheduler.ref_cancel(self._schedref)
    if path is not None:
      pyst.to_state(self, path)

  @staticmethod
  def load(path, **kwargs):
    return pyst.from_state(__class__, path, **kwargs)

  def get_account(self):
    return api_types.Account(id=self._api_key,
                             buying_power=self._capital)

  def get_market_hours(self, dt):
    return mh.get_market_hours(dt)

  def now(self):
    return pyd.from_timestamp(self.scheduler.timegen.now())

  # Requires lock!
  def _try_fill(self, order_id, symbol, quantity, side, type):
    price = self._prices.get(symbol)
    tas.check_is_not_none(price, msg=f'Missing price information for symbol: {symbol}')

    if side == 'buy':
      filled_quantity = min(quantity, int(self._capital / price.price))

      if filled_quantity:
        self._positions[symbol].append(Position(symbol=symbol,
                                                quantity=filled_quantity,
                                                price=price.price,
                                                timestamp=self.now(),
                                                order_id=order_id))
        self._capital -= filled_quantity * price.price
    elif side == 'sell':
      positions = self._positions.get(symbol, [])

      filled_quantity, changes = 0, []
      for i, p in enumerate(positions):
        qleft = quantity - filled_quantity
        if p.quantity > qleft:
          pos_quantity = qleft
          changes.append((i, pycu.new_with(p, quantity=p.quantity - qleft)))
        else:
          pos_quantity = p.quantity
          changes.append((i, None))

        alog.debug0(f'Selling {pos_quantity} units of {symbol} bought at ' \
                    f'{p.price:.2f} US$ (order #{p.order_id}), for {price.price:.2f} US$ ' \
                    f'... gain is {(price.price - p.price) * pos_quantity:.2f} US$')

        filled_quantity += pos_quantity
        if filled_quantity >= quantity:
          break

      # Make sure we pop in reverse order to keep indices valid.
      for i, np in reversed(changes):
        if np is None:
          positions.pop(i)
        else:
          positions[i] = np

      self._capital += filled_quantity * price.price
    else:
      alog.xraise(RuntimeError, f'Unknown order side: {side}')

    alog.debug0(f'New capital for "{self._api_key}" is {self._capital:.2f} US$')

    return filled_quantity, price

  def _fill_quantity(self, quantity, filled):
    qleft = quantity - filled
    qpct = max(1, int(quantity * self._fill_pct)) if self._fill_pct else qleft

    return min(qleft, qpct)

  def _schedule_fill(self, order_id):
    self.scheduler.enter(self._fill_delay, self._try_fill_order,
                         ref=self._schedref, argument=(order_id,))

  def _order_status(self, to_be_filled, filled_quantity, current_fill, quantity):
    if filled_quantity < to_be_filled:
      return 'truncated'

    return 'filled' if current_fill == quantity else 'partially_filled'

  def _try_fill_order(self, order_id):
    with self._lock:
      order = self._orders.get(order_id)
      if order is not None and order.status not in {'filled', 'truncated'}:
        to_be_filled = self._fill_quantity(order.quantity, order.filled_quantity)
        filled_quantity, price = self._try_fill(order_id,
                                                order.symbol,
                                                to_be_filled,
                                                order.side,
                                                order.type)

        current_fill = order.filled_quantity + filled_quantity
        avg_price = (order.filled_avg_price * order.filled_quantity +
                     price.price * filled_quantity) / current_fill
        status = self._order_status(to_be_filled, filled_quantity, current_fill,
                                    order.quantity)

        self._orders[order_id] = pycu.new_with(order,
                                               filled_quantity=current_fill,
                                               filled=self.now(),
                                               status=status,
                                               filled_avg_price=avg_price)

        if status == 'partially_filled':
          self._schedule_fill(order_id)

  def _submit_stop_order(self, symbol, quantity, side, type, stop):
    price = self._prices.get(symbol)
    now = self.now()

    order = Order(id=self._order_id,
                  symbol=symbol,
                  quantity=quantity,
                  side=side,
                  type=type,
                  stop=stop,
                  created=now,
                  filled=now,
                  ref_price=price.price)

    self._orders[order.id] = order
    self._order_id += 1
    self._stops[symbol].append(order)

    return order

  def _check_stops_ready(self, symbol, price):
    stops, ready = self._stops.get(symbol, []), []
    for i, order in enumerate(stops):
      # Check for price crossing the stop value.
      if (order.ref_price - order.stop) * (order.stop - price) >= 0:
        ready.append(i)

    for i in reversed(ready):
      order = stops[i]
      alog.debug0(f'Stop order triggered: {order}')

      stops.pop(i)

      self._schedule_fill(order.id)

  def _submit_order(self, symbol, quantity, side, type):
    to_be_filled = self._fill_quantity(quantity, 0)
    filled_quantity, price = self._try_fill(self._order_id,
                                            symbol,
                                            to_be_filled,
                                            side,
                                            type)

    now = self.now()
    status = self._order_status(to_be_filled, filled_quantity, filled_quantity,
                                quantity)

    order = Order(id=self._order_id,
                  symbol=symbol,
                  quantity=quantity,
                  side=side,
                  type=type,
                  status=status,
                  created=now,
                  filled=now,
                  filled_quantity=filled_quantity,
                  filled_avg_price=price.price)

    self._orders[order.id] = order
    self._order_id += 1

    if status == 'partially_filled':
      self._schedule_fill(order.id)

    return order

  def submit_order(self, symbol, quantity, side, type='market', limit=None, stop=None):
    tas.check_eq(type, 'market', msg=f'Order type not supported: type={type}')
    tas.check_is_none(limit, msg=f'Limit orders not supported: limit={limit}')

    with self._lock:
      if stop is not None:
        order = self._submit_stop_order(symbol, quantity, side, type, stop)
      else:
        order = self._submit_order(symbol, quantity, side, type)

    return _marshal_order(order)

  def get_order(self, oid):
    with self._lock:
      order = self._orders.get(oid)

    return _marshal_order(order) if order is not None else None

  def _match_status(self, order, status):
    if status == 'all':
      return True
    if status == 'open':
      return order.status in {'new', 'partially_filled'}
    if status == 'closed':
      return order.status in {'filled', 'canceled', 'truncated'}

    alog.xraise(ValueError, f'Unknown status select status: "{status}"')

  def list_orders(self, limit=None, status='all', start_date=None, end_date=None):
    if end_date is None:
      end_date = self.now()
    if start_date is None:
      start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    orders = []
    with self._lock:
      for oid, order in self._orders.items():
        if (order.created > start_date and order.created < end_date and
            self._match_status(order, status)):
          orders.append(order)

    orders = sorted(orders, key=lambda o: o.created)
    if limit is not None:
      orders = orders[-limit:]

    return [_marshal_order(o) for o in orders]

  def cancel_order(self, oid):
    with self._lock:
      order = self._orders.get(oid)
      if order is not None and order.status in {'new', 'partially_filled'}:
        self._orders[oid] = pycu.new_with(order, status='canceled')

  def list_positions(self):
    with self._lock:
      positions = []
      for sym, sym_positions in self._positions.items():
        positions.extend(_marshal_position(p) for p in sym_positions)

    return positions

  def fetch_data(self, symbols, start_date, end_date, data_step='5Min', dtype=None):
    pass

  def handle_trade(self, t):
    with self._lock:
      price = self._prices.get(t.symbol)
      if price is None or t.timestamp > price.timestamp:
        self._prices[t.symbol] = Price(price=t.price, timestamp=t.timestamp)
        self.scheduler.timegen.set_time(t.timestamp)

        self._check_stops_ready(t.symbol, t.price)

  def handle_bar(self, b):
    with self._lock:
      price = self._prices.get(b.symbol)
      if price is None or b.timestamp > price.timestamp:
        self._prices[b.symbol] = Price(price=b.close, timestamp=b.timestamp)
        self.scheduler.timegen.set_time(b.timestamp)

        self._check_stops_ready(b.symbol, b.close)

  def handle_symbars(self, bars):
    prices = dict()
    for sym, sdf in bars.items():
      times = sdf['t'].to_numpy()
      if len(times) > 0:
        ilast = np.argmax(times)
        cdata = sdf.get(f'{sym}.c')
        if cdata is None:
          cdata = sdf['c']
        close_prices = cdata.to_numpy()
        prices[sym] = Price(price=float(close_prices[ilast]),
                            timestamp=times[ilast])

    current_time = -1
    with self._lock:
      for sym, bprice in prices.items():
        price = self._prices.get(sym)
        if price is None or bprice.timestamp > price.timestamp:
          self._prices[sym] = bprice
          current_time = max(bprice.timestamp, current_time)

          self._check_stops_ready(sym, bprice.price)

    if current_time > 0:
      self.scheduler.timegen.set_time(current_time)

