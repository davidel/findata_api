import collections
import datetime
import os
import pickle
import threading

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import file_overwrite as fow
from py_misc_utils import pd_utils as pyp
from py_misc_utils import scheduler as sch
from py_misc_utils import utils as pyu

from . import api_base
from . import api_types
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
    'id, symbol, quantity, side, type, limit, stop, status, created, filled, filled_quantity, filled_avg_price')
Position = collections.namedtuple('Position', 'symbol, quantity, price, timestamp, order_id')


class API(api_base.API):

  def __init__(self, api_key, capital, path, scheduler=None, fill_pct=None,
               fill_delay=None):
    super().__init__()
    self._api_key = api_key
    self._capital = capital
    self._path = path
    self._scheduler = scheduler or sch.common_scheduler()
    self._fill_pct = fill_pct
    self._fill_delay = fill_delay or 1.0
    self._schedref = self._scheduler.gen_unique_ref()
    self._lock = threading.Lock()
    self._prices = dict()
    self._orders = dict()
    self._positions = collections.defaultdict(list)
    self._order_id = 1

    state_path = self._state_path()
    if os.path.isfile(state_path):
      self._load_state(state_path)

  def _load_state(self, path):
    with open(path, mode='rb') as sfd:
      state = pickle.load(sfd)

    self._capital = state['capital']
    self._prices = state['prices']
    self._orders = state['orders']
    self._positions = state['positions']
    self._order_id = state['order_id']

  def _state_path(self):
    return os.path.join(self._path, self._api_key)

  def save_state(self):
    with self._lock:
      state = dict(capital=self._capital,
                   prices=self._prices,
                   orders=self._orders,
                   positions=self._positions,
                   order_id=self._order_id)

    with fow.FileOverwrite(self._state_path(), mode='wb') as sfd:
      pickle.dump(state, sfd, protocol=pyu.pickle_proto())

  def close(self):
    self._scheduler.ref_cancel(self._schedref)
    self.save_state()

  @property
  def name(self):
    return 'Paper'

  @property
  def supports_trading(self):
    return True

  def get_account(self):
    return api_types.Account(id=self._api_key,
                             buying_power=self._capital)

  def get_market_hours(self, dt):
    return ut.get_market_hours(dt)

  # Requires lock!
  def _try_fill(self, symbol, quantity, side, type, limit, stop):
    price = self._prices.get(symbol, None)
    tas.check_is_not_none(price, msg=f'Missing price information for symbol: {symbol}')

    if side == 'buy':
      filled_quantity = min(quantity, int(self._capital / price.price))

      if filled_quantity:
        self._positions[symbol].append(Position(symbol=symbol,
                                                quantity=filled_quantity,
                                                price=price.price,
                                                timestamp=pyd.now(),
                                                order_id=self._order_id))
        self._capital -= filled_quantity * price.price
    elif side == 'sell':
      positions = self._positions.get(symbol, [])

      filled_quantity, changes = 0, []
      for i, p in enumerate(positions):
        qleft = quantity - filled_quantity
        if p.quantity > qleft:
          pos_quantity = qleft
          changes.append((i, pyu.new_with(p, quantity=p.quantity - qleft)))
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
      changes.reverse()
      for i, np in changes:
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

  def _try_fill_order(self, order_id):
    with self._lock:
      order = self._orders.get(order_id, None)
      if order is not None and order.status != 'filled':
        filled_quantity, price = self._try_fill(order.symbol,
                                                self._fill_quantity(order.quantity,
                                                                    order.filled_quantity),
                                                order.side,
                                                order.type,
                                                order.limit,
                                                order.stop)

        current_fill = order.filled_quantity + filled_quantity
        status = 'filled' if current_fill == order.quantity else 'partially_filled'
        self._orders[order_id] = pyu.new_with(order,
                                              filled_quantity=current_fill,
                                              filled=pyd.now(),
                                              status=status)

        if current_fill < order.quantity:
          self._scheduler.enter(self._fill_delay, self._try_fill_order,
                                ref=self._schedref, argument=(order_id,))

  def submit_order(self, symbol, quantity, side, type='market', limit=None, stop=None):
    tas.check_eq(type, 'market', msg=f'Order type not supported: type={type}')
    tas.check_is_none(limit, msg=f'Limit orders not supported: limit={limit}')
    tas.check_is_none(stop, msg=f'Stop orders not supported: stop={stop}')

    with self._lock:
      filled_quantity, price = self._try_fill(symbol,
                                              self._fill_quantity(quantity, 0),
                                              side,
                                              type,
                                              limit,
                                              stop)

      now = pyd.now()
      status = 'filled' if filled_quantity == quantity else 'partially_filled'
      order = Order(id=self._order_id,
                    symbol=symbol,
                    quantity=quantity,
                    side=side,
                    type=type,
                    limit=limit,
                    stop=stop,
                    status=status,
                    created=now,
                    filled=now,
                    filled_quantity=filled_quantity,
                    filled_avg_price=price.price)

      self._orders[self._order_id] = order
      self._order_id += 1

      if filled_quantity < quantity:
        self._scheduler.enter(self._fill_delay, self._try_fill_order,
                              ref=self._schedref, argument=(order.id,))

    return _marshal_order(order)

  def get_order(self, oid):
    with self._lock:
      order = self._orders.get(oid, None)

    return _marshal_order(order) if order is not None else None

  def list_orders(self, limit=None, status='all', start_date=None, end_date=None):
    if end_date is None:
      end_date = pyd.now()
    if start_date is None:
      start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

    orders = []
    with self._lock:
      for oid, order in self._orders.items():
        if (order.created > start_date and order.created < end_date and
            (status == 'all' or status == order.status)):
          orders.append(order)

    orders = sorted(orders, key=lambda o: o.created)
    if limit is not None:
      orders = orders[-limit:]

    return [_marshal_order(o) for o in orders]

  def cancel_order(self, oid):
    with self._lock:
      order = self._orders.get(oid, None)
      if order is not None and order.status in {'new', 'partially_filled'}:
        self._orders[oid] = pyu.new_with(order, status='canceled')

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
      price = self._prices.get(t.symbol, None)
      if price is None or t.timestamp > price.timestamp:
        self._prices[t.symbol] = Price(price=t.price, timestamp=t.timestamp)

  def handle_bar(self, b):
    with self._lock:
      price = self._prices.get(b.symbol, None)
      if price is None or b.timestamp > price.timestamp:
        self._prices[b.symbol] = Price(price=b.close, timestamp=b.timestamp)

  def handle_symbars(self, bars):
    prices = dict()
    for sym, sdf in bars.items():
      times = sdf['t'].to_numpy()
      if times:
        ilast = np.argmax(times)
        cdata = sdf.get(f'{sym}.c', None)
        if cdata is None:
          cdata = sdf['c']
        close_prices = cdata.to_numpy()
        prices[sym] = Price(price=float(close_prices[ilast]),
                            timestamp=times[ilast])

    with self._lock:
      for sym, bprice in prices.items():
        price = self._prices.get(sym, None)
        if price is None or bprice.timestamp > price.timestamp:
          self._prices[sym] = bprice

