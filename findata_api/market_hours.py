import datetime

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import pd_utils as pyp


class MarketTimeTracker:

  def __init__(self, open_delta=0, close_delta=0, market=None):
    self._open_delta = open_delta
    self._close_delta = close_delta
    self._cal = mcal.get_calendar(market or 'NYSE')
    self._tdb = dict()
    self._range_start, self._range_end = None, None
    self._last_ds, self._last_times = 0, ()

  def _load_range(self, dt):
    delta = datetime.timedelta(days=60)
    if self._range_start is None:
      self._range_start, self._range_end = _noon(dt - delta), _noon(dt + delta)

      return self._range_start, self._range_end

    if dt < self._range_start:
      end_range = self._range_start
      self._range_start = _noon(dt - delta)

      return self._range_start, end_range

    if dt > self._range_end:
      start_range = self._range_end
      self._range_end = _noon(dt + delta)

      return start_range, self._range_end

  def _prefetch(self, dt):
    range = self._load_range(dt)
    if range:
      start_date, end_date = range

      df = self._cal.schedule(start_date=_ktime(start_date),
                              end_date=_ktime(end_date))
      df = df.sort_values('market_open', ignore_index=True)

      mop = pd.to_datetime(df['market_open']).dt.tz_convert(self._cal.tz)
      mcl = pd.to_datetime(df['market_close']).dt.tz_convert(self._cal.tz)

      for o, c in zip(mop, mcl):
        odt, cdt = o.to_pydatetime(), c.to_pydatetime()
        self._tdb[_norm_timestamp(odt)] = (odt.timestamp(), cdt.timestamp())

  def _market_times(self, dt):
    ds = _norm_timestamp(dt)
    times = self._tdb.get(ds)
    if times is None:
      self._prefetch(dt)
      times = self._tdb.get(ds)
      if times is None:
        self._tdb[ds] = ()

    return ds, times

  def open_at(self, dt):
    ldt = dt.astimezone(self._cal.tz)
    _, times = self._market_times(ldt)

    return len(times) == 2 and (times[0] <= ldt.timestamp() < times[1])

  def _get_entry(self, t):
    # 86400 = Seconds per day.
    if 0 <= (t - self._last_ds) < 86400:
      return self._last_ds, self._last_times

    dt = pyd.from_timestamp(t, tz=self._cal.tz)
    self._last_ds, self._last_times = self._market_times(dt)

    return self._last_ds, self._last_times

  def _is_open(self, t, times):
    if not times:
      return False

    open_offset = times[0] + self._open_delta
    close_offset = times[1] + self._close_delta

    return open_offset <= t < close_offset

  def filter(self, t):
    ds, times = self._get_entry(t)

    return self._is_open(t, times)

  def filter2(self, t):
    ds, times = self._get_entry(t)

    return self._is_open(t, times), ds

  def elapsed(self, t0, t1):
    if t0 > t1:
      t0, t1 = t1, t0
      sign = -1
    else:
      sign = 1

    ds0, times0 = self._get_entry(t0)
    ds1, times1 = self._get_entry(t1)
    if ds0 == ds1:
      if times0:
        ct0 = np.clip(t0, times0[0], times0[1]) - ds0
        ct1 = np.clip(t1, times1[0], times1[1]) - ds1

        return sign * (ct1 - ct0)

      return 0

    if times0:
      elapsed = times0[1] - np.clip(t0, times0[0], times0[1])
    else:
      elapsed = 0

    tc = t0
    while True:
      tc += 86400
      ds, times = self._get_entry(tc)
      if ds >= ds1:
        if times and ds == ds1:
          elapsed += np.clip(tc, times[0], times[1]) - times[0]

        break

      if times:
        elapsed += times[1] - times[0]

    return sign * elapsed


def _ktime(dt):
  return dt.strftime('%Y-%m-%d')


def _noon(dt):
  return dt.replace(hour=12, minute=0, second=0, microsecond=0)


def _norm_timestamp(dt):
  # Use noon as reference time for midnight rounding. This only differs during
  # DST change days. The 43200 is the number of seconds from midnight to noon.
  return round(_noon(dt).timestamp()) - 43200


def market_hours(dt, market=None):
  cal = mcal.get_calendar(market or 'NYSE')

  dtz = dt.astimezone(cal.tz)
  kt = _ktime(dtz)

  df = cal.schedule(start_date=kt, end_date=kt)
  if df:
    mop = pd.to_datetime(df['market_open']).dt.tz_convert(cal.tz)
    mcl = pd.to_datetime(df['market_close']).dt.tz_convert(cal.tz)

    times = mop[0].to_pydatetime(), mcl[0].to_pydatetime()
  else:
    times = ()

  return dtz, times


def is_market_open(dt, market=None):
  dtz, times = market_hours(dt, market=market)

  return times[0] <= dtz < times[1] if times else False


def get_market_hours(dt, market=None):
  _, times = market_hours(dt, market=market)

  return times


def market_filter(df, epoch_col, open_delta=0, close_delta=0):
  mkf = MarketTimeTracker(open_delta=open_delta, close_delta=close_delta)

  alog.debug0(f'Market-filtering {len(df)} records ...')

  mask = [mkf.filter(t) for t in pyp.column_or_index(df, epoch_col)]
  mdf = df[mask]
  if mdf.index.name is None:
    mdf = mdf.reset_index(drop=True)

  alog.info(f'Market-filtered {len(mdf)} out of {len(df)}')

  return mdf

