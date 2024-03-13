import array
import collections
import sys
import threading

import numpy as np
from py_misc_utils import alog
from py_misc_utils import named_array as nar
from py_misc_utils import utils as pyu


_TRADES_FIELDS = ('timestamps', 'prices', 'volume')
_TRADES_FMT = 'ddd'

_QUOTES_FIELDS = ('timestamps', 'bid_sizes', 'bid_prices', 'ask_sizes', 'ask_prices')
_QUOTES_FMT = 'ddddd'


class BarStorage:

  def __init__(self):
    self._bars = []
    self._sorted = True

  def bars(self):
    return self._bars

  def add(self, x):
    self._bars.append(x)
    self._sorted = False

  def sort(self):
    if not self._sorted:
      self._bars.sort(key=lambda x: x.timestamp)
      self._sorted = True


def _trades_array():
  return nar.NamedArray(_TRADES_FIELDS, fmt=_TRADES_FMT)


def _quotes_array():
  return nar.NamedArray(_QUOTES_FIELDS, fmt=_QUOTES_FMT)


def _get_bound_times(start_ts, end_ts):
  if start_ts is None:
    start_ts = 0
  if end_ts is None:
    end_ts = sys.maxsize

  return start_ts, end_ts


def _get_timerange_indices(timestamps, start_ts, end_ts):
  np_timestamps = np.array(timestamps)
  indices = np.argsort(np_timestamps)
  pos = pyu.bisect_left(start_ts, lambda i: np_timestamps[indices[i]], len(indices))
  for i in range(pos, len(indices)):
    x = indices[i]
    if np_timestamps[x] >= end_ts:
      break
    yield x


class Bar:

  def __init__(self, symbol, data, timestamp=None):
    self._symbol = symbol
    self._data = data
    self._timestamp = timestamp

  @property
  def timestamp(self):
    return self._timestamp

  def flush(self):
    # If the timestamp is not specified at constructor time, we use the last one.
    if self._timestamp is None:
      timestamps = self._data.get_array('timestamps')
      self._timestamp = np.amax(timestamps)

    return self


class TradeBar(Bar):

  def __init__(self, symbol, timestamp=None):
    super().__init__(symbol, _trades_array(), timestamp=timestamp)

  def update(self, t):
    self._data.append(t.timestamp, t.price, t.quantity)

  def get_values(self, start_ts, end_ts):
    # They are returned in _TRADES_FIELDS order!
    timestamps, prices, volumes = self._data.get_arrays()
    values = _trades_array()
    for x in _get_timerange_indices(timestamps, start_ts, end_ts):
      values.append(timestamps[x], prices[x], volumes[x])

    return values

  def get_bar(self):
    # They are returned in _TRADES_FIELDS order!
    timestamps, prices, volumes = self._data.get_arrays()
    indices = np.argsort(timestamps)
    np_prices, np_volumes = np.array(prices), np.array(volumes)
    total_volume = np_volumes.sum()
    avg_price = (np_prices * np_volumes).sum() / total_volume

    return dict(t=self._timestamp,
                o=np_prices[indices[0]],
                h=np_prices.max(),
                l=np_prices.min(),
                c=np_prices[indices[-1]],
                v=total_volume,
                a=avg_price,
                n=len(timestamps),
                symbol=self._symbol)


class QuoteBar(Bar):

  def __init__(self, symbol, timestamp=None):
    super().__init__(symbol, _quotes_array(), timestamp=timestamp)

  def update(self, q):
    self._data.append(q.timestamp, q.bid_size, q.bid_price, q.ask_size, q.ask_price)

  def get_values(self, start_ts, end_ts):
    # They are returned in _QUOTES_FIELDS order!
    timestamps, bid_sizes, bid_prices, ask_sizes, ask_prices = self._data.get_arrays()
    values = _quotes_array()
    for x in _get_timerange_indices(timestamps, start_ts, end_ts):
      values.append(timestamps[x], bid_sizes[x], bid_prices[x],
                    ask_sizes[x], ask_prices[x])

    return values


class StreamComputer:

  def __init__(self, bar_interval=None, volume_interval=None, symbols=None):
    assert bar_interval is not None or volume_interval is not None
    self._bar_interval = bar_interval
    self._volume_interval = volume_interval
    self._symbols = set(symbols) if symbols else None
    self._lock = threading.Lock()
    self._trade_bars = collections.defaultdict(BarStorage)
    self._quote_bars = collections.defaultdict(BarStorage)
    self._active_trade_bars = dict()
    self._active_quote_bars = dict()
    if bar_interval is not None:
      self.trade_handler = self._timebar_trade_handler
      self.quote_handler = self._timebar_quote_handler
    elif volume_interval is not None:
      self.trade_handler = self._volumebar_trade_handler
      self.quote_handler = self._volumebar_quote_handler

  def get_trade_bars(self, start_ts=None, end_ts=None):
    start_ts, end_ts = _get_bound_times(start_ts, end_ts)
    bars = collections.defaultdict(list)
    with self._lock:
      for sym, bars_stg in self._trade_bars.items():
        xbars = bars[sym]
        for rbar in bars_stg.bars():
          if start_ts <= rbar.timestamp < end_ts:
            xbars.append(rbar.get_bar())

    return bars

  def _purge_bars_dict(self, bd, upto_ts):
    purged = collections.defaultdict(list)
    for symbol, bars_stg in bd.items():
      pg_sbars = purged[symbol]
      left = BarStorage()
      for rbar in bars_stg.bars():
        if rbar.timestamp < upto_ts:
          pg_sbars.append(rbar)
        else:
          left.add(rbar)

      bd[symbol] = left

    return purged

  def purge_oldest(self, upto_ts):
    with self._lock:
      trades_purged = self._purge_bars_dict(self._trade_bars, upto_ts)
      quotes_purged = self._purge_bars_dict(self._quote_bars, upto_ts)

      return pyu.make_object(trades=trades_purged, quotes=quotes_purged)

  def _get_values(self, bars_stg, start_ts, end_ts):
    bars_stg.sort()
    sbars = bars_stg.bars()

    pos = pyu.bisect_left(start_ts, lambda i: sbars[i].timestamp, len(sbars))
    values = None
    for i in range(pos, len(sbars)):
      rbar = sbars[i]
      if rbar.timestamp >= end_ts:
        break
      pvalues = rbar.get_values(start_ts, end_ts)
      if values is not None:
        values.exend_ts(pvalues)
      else:
        values = pvalues

    return values

  def get_trades(self, symbol, start_ts=None, end_ts=None):
    start_ts, end_ts = _get_bound_times(start_ts, end_ts)
    with self._lock:
      return self._get_values(self._trade_bars[symbol], start_ts, end_ts)

  def get_quotes(self, symbol, start_ts=None, end_ts=None):
    start_ts, end_ts = _get_bound_times(start_ts, end_ts)
    with self._lock:
      return self._get_values(self._quote_bars[symbol], start_ts, end_ts)

  def flush(self):
    with self._lock:
      for sym, cbar in self._active_trade_bars.items():
        self._trade_bars[sym].add(cbar.bar.flush())

      self._active_trade_bars.clear()

      for sym, cbar in self._active_quote_bars.items():
        self._quote_bars[sym].add(cbar.bar.flush())

      self._active_quote_bars.clear()

  def _timebar_handler(self, bars, abars, bar_ctor, x):
    if self._symbols is None or x.symbol in self._symbols:
      # Use the end time of the bar as timestamp
      ts = (int(x.timestamp) // self._bar_interval) * self._bar_interval + self._bar_interval
      with self._lock:
        cbar = abars.get(x.symbol, None)
        if cbar is None or ts >= cbar.bar.timestamp:
          if cbar is not None:
            bars[x.symbol].add(cbar.bar.flush())
          cbar = pyu.make_object(bar=bar_ctor(x.symbol, timestamp=ts))
          abars[x.symbol] = cbar

        cbar.bar.update(x)

  def _timebar_trade_handler(self, t):
    self._timebar_handler(self._trade_bars, self._active_trade_bars, TradeBar, t)

  def _timebar_quote_handler(self, q):
    self._timebar_handler(self._quote_bars, self._active_quote_bars, QuoteBar, q)

  def _volumebar_trade_handler(self, t):
    if self._symbols is None or t.symbol in self._symbols:
      volume_interval = self._volume_interval
      if isinstance(volume_interval, dict):
        volume_interval = volume_interval[t.symbol]
      volume = t.price * t.quantity
      with self._lock:
        cbar = self._active_trade_bars.get(t.symbol, None)
        if cbar is None or (cbar.volume + volume) > volume_interval:
          if cbar is not None:
            self._trade_bars[t.symbol].add(cbar.bar.flush())

          # For volume bars we do not pass the timestamp to the TradeBar constructor.
          # This will make the bar to get a timestamp which is the last timestamp of
          # the trade within the volume bar.
          cbar = pyu.make_object(bar=TradeBar(t.symbol), volume=0.0)
          self._active_trade_bars[t.symbol] = cbar

        cbar.bar.update(t)
        cbar.volume += volume

  def _volumebar_quote_handler(self, q):
    alog.xraise(RuntimeError, f'Volume bars not supported for quotes')
