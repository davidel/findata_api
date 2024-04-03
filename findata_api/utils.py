import array
import bisect
import collections
import datetime
import math
import os
import re
import time

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import np_utils as pyn
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu


class MarketTimeTracker:

  def __init__(self, open_delta=0, close_delta=0, market=None, fetch_days=None):
    self._open_delta = open_delta
    self._close_delta = close_delta
    self._fetch_days = fetch_days or 15
    self._cal = mcal.get_calendar(market or 'NYSE')
    self._tdb = dict()
    self._last_ds, self._last_times = 0, ()

  def _load_range(self, dt):
    delta = datetime.timedelta(days=self._fetch_days)

    return dt - delta, dt + delta

  def _prefetch(self, dt):
    start_date, end_date = self._load_range(dt)

    df = self._cal.schedule(start_date=_ktime(start_date),
                            end_date=_ktime(end_date))
    df = df.sort_values('market_open', ignore_index=True)

    mop = pd.to_datetime(df['market_open']).dt.tz_convert(self._cal.tz)
    moc = pd.to_datetime(df['market_close']).dt.tz_convert(self._cal.tz)

    last_o = start_date - datetime.timedelta(days=1)
    for o, c in zip(mop, moc):
      o, c = o.to_pydatetime(), c.to_pydatetime()

      ods = _norm_timestamp(o)
      assert ods == _norm_timestamp(c)

      ht = last_o + datetime.timedelta(days=1)
      while True:
        hds = _norm_timestamp(ht)
        if hds == ods or hds in self._tdb:
          break
        else:
          self._tdb[hds] = ()
          ht += datetime.timedelta(days=1)

      self._tdb[ods] = (o.timestamp(), c.timestamp())

      last_o = o

  def _market_times(self, dt):
    ds = _norm_timestamp(dt)
    times = self._tdb.get(ds, None)
    if times is None:
      self._prefetch(dt)
      times = self._tdb[ds]

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
      if ds == ds1:
        if times:
          elapsed += np.clip(tc, times[0], times[1]) - times[0]

        break

      if times:
        elapsed += times[1] - times[0]

    return sign * elapsed


def _ktime(dt):
  return dt.strftime('%Y-%m-%d')


def _norm_timestamp(dt):
  return round(dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())


def market_hours(dt, market=None):
  cal = mcal.get_calendar(market or 'NYSE')

  dtz = dt.astimezone(cal.tz)
  kt = _ktime(dtz)

  df = cal.schedule(start_date=kt, end_date=kt)
  if df:
    mop = pd.to_datetime(df['market_open']).dt.tz_convert(cal.tz)
    moc = pd.to_datetime(df['market_close']).dt.tz_convert(cal.tz)

    times = mop[0].to_pydatetime(), moc[0].to_pydatetime()
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


def day_split_exec(df, epoch_col, exec_fn, exec_args=None, open_delta=0,
                   close_delta=0, max_gap=None):
  mkf = MarketTimeTracker(open_delta=open_delta, close_delta=close_delta)

  splits = collections.defaultdict(lambda: array.array('L'))
  times = pyn.to_numpy(pyp.column_or_index(df, epoch_col))
  for i, t in enumerate(times):
    fok, ds = mkf.filter2(t)
    if fok:
      splits[ds].append(i)

  exec_args = exec_args or dict()
  result = []

  def push_result(indx):
    mdf = pyp.dataframe_rows_select(df, indx)
    if mdf.index.name is None:
      mdf = mdf.reset_index(drop=True)

    result.append(exec_fn(mdf, **exec_args))

  for t, indices in splits.items():
    alog.debug0(f'Split has {len(indices)} rows at {pyd.from_timestamp(t).isoformat()}')

    if max_gap is None:
      push_result(indices)
    else:
      np_indices = np.array(indices)
      dtimes = times[np_indices]

      dsplits = pyn.diff_split(dtimes, lambda x: x > max_gap)
      for sidx in dsplits:
        push_result(np_indices[sidx])

  return result


def split_field(f):
  # The fields we build for feature generation (whose names are appended to the
  # original field name after a '__' sequence) can contain dots which needs to
  # be taken care when splitting the symbol and field names.
  xpos = f.find('__')
  pos = f.rfind('.', 0, xpos if xpos > 0 else len(f))

  return (f[: pos], f[pos + 1:]) if pos >= 0 else (None, f)


def split_columns(cols):
  symbols = collections.defaultdict(list)
  for c in cols:
    sym, field = split_field(c)
    if sym and field:
      symbols[sym].append(field)

  return symbols


def normalize_columns(cols=None, df=None):
  if cols is None:
    cols = df.columns
  symbols, new_cols = dict(), []
  for c in cols:
    sym, field = split_field(c)
    if sym and field:
      sid = symbols.get(sym, None)
      if sid is None:
        sid = f'S{len(symbols)}'
        symbols[sym] = sid
      new_cols.append(f'{sid}.{field}')
    else:
      new_cols.append(c)

  return new_cols, symbols


def get_df_columns_symbols(df):
  return sorted(split_columns(df.columns).keys())


def get_df_column_unique(df, name):
  return sorted(df[name].unique().tolist())


def get_data_step_delta(data_step):
  if isinstance(data_step, datetime.timedelta):
    return data_step

  m = re.match(r'(\d+(\.\d+)?)?([a-z]+)', data_step.lower())
  if not m:
    alog.xraise(RuntimeError, f'Invalid data step: {data_step}')
  count, unit = m.group(1), m.group(3)
  n = float(count) if count else 1
  if unit in {'s', 'sec', 'second'}:
    return datetime.timedelta(seconds=n)
  if unit in {'m', 'min', 'minute'}:
    return datetime.timedelta(minutes=n)
  if unit in {'h', 'hour'}:
    return datetime.timedelta(hours=n)
  if unit in {'d', 'day'}:
    return datetime.timedelta(days=n)
  if unit in {'w', 'wk', 'week'}:
    return datetime.timedelta(days=n * 7)
  if unit in {'mo', 'month'}:
    return datetime.timedelta(days=n * 30)

  alog.xraise(RuntimeError, f'Invalid data step: {data_step}')


def map_data_step(ds, maps):
  lds = ds.lower()
  m = re.match(r'(\d*)([a-zA-Z].*)$', lds)
  if m:
    mu = maps.get(m.group(2), m.group(2))
    return m.group(1) + mu

  return lds


def break_period_in_dates_list(start_date, end_date, step):
  dates = []
  while end_date > start_date:
    dates.append((start_date, min(start_date + step, end_date)))
    start_date += step

  return tuple(dates)


def time_filter_dataframe(df, start_date=None, end_date=None, col=None):
  values = get_dataframe_index_time(df, col=col)

  if start_date:
    if end_date:
      mask = (values >= start_date.timestamp()) & (values <= end_date.timestamp())
    else:
      mask = values >= start_date.timestamp()
  elif end_date:
    mask = values <= end_date.timestamp()
  else:
    alog.debug1(f'Nothing to filter')
    return df

  fdf = df[mask]

  alog.debug0(f'Time-filtered {len(df) - len(fdf)} records')

  return fdf


def dataframe_column_time_shift(df, name, delta):
  seconds = get_data_step_delta(delta).total_seconds()

  def adjust(t):
    if isinstance(t, np.datetime64):
      dec_part, int_part = math.modf(seconds)
      dt = np.timedelta64(int(1e6 * dec_part), 'us') + np.timedelta64(int(int_part), 's')
      return t + dt

    return t + int(seconds) if np.issubdtype(t.dtype, np.integer) else t + seconds

  pyp.dataframe_column_rewrite(df, name, adjust)


def purge_fetched_data(df, start_date, end_date, data_step):
  alog.debug0(f'Sorting by time')
  df = df.sort_values('t', ignore_index=True)

  if start_date or end_date:
    alog.debug0(f'Time filtering fetched data')
    df = time_filter_dataframe(df, start_date=start_date, end_date=end_date, col='t')

  # We assign the bar timestamp to the end of the bar, to make it consistent with
  # volume/tick bars, for which an ending timestamp make more sense.
  dataframe_column_time_shift(df, 't', data_step)

  return df.reset_index(drop=True)


def get_sp500_symbols():
  pds = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
  # There are 2 tables on the Wikipedia page, get the first table.
  stable = pds[0]

  return sorted(stable['Symbol'].tolist())


def parse_time_range(start_date, end_date, minutes=None, tz=None):
  if minutes is None:
    start = pyd.parse_date(start_date, tz=tz)
    if end_date:
      end = pyd.parse_date(end_date, tz=tz)
    else:
      end = pyd.now(tz=tz)
  elif start_date:
    start = pyd.parse_date(start_date, tz=tz)
    end = start + datetime.timedelta(minutes=minutes)
  else:
    if end_date:
      end = pyd.parse_date(end_date, tz=tz)
    else:
      end = pyd.now(tz=tz)
    start = end - datetime.timedelta(minutes=minutes)

  return start, end


def convert_to_epoch(values, dtype=np.float64):
  values = pyn.to_numpy(values)
  if values.size == 0:
    return np.empty((0,), dtype=dtype)

  if isinstance(values[0], str):
    values = pd.to_datetime(values)
  if isinstance(values[0], datetime.datetime):
    return np.array([x.timestamp() for x in values], dtype=dtype)
  if isinstance(values[0], pd.Timestamp):
    values = np.array([x.to_numpy() for x in values])
  if np.issubdtype(values.dtype, np.datetime64):
    return pyd.np_datetime_to_epoch(values, dtype=dtype)

  # At this point values better already contain an EPOCH value (although
  # with different dtype).
  return values.astype(dtype)


def get_dataframe_index_time(df, col=None, dtype=np.float64):
  values = pyp.column_or_index(df, col or 't')
  if values is not None:
    return convert_to_epoch(values, dtype=dtype)


def _scan_column(data, row, col, fn):
  # Try to propagate previous values first.
  for r in range(row - 1, -1, -1):
    v = data[r, col]
    if fn(v):
      return v
  # Then try next values.
  for r in range(row + 1, len(data)):
    v = data[r, col]
    if fn(v):
      return v


def _is_hole(v):
  return (v == 0) | np.isnan(v)


def fixup_dataframe(df):
  # Fixup the input data to avoid that normalization finds unwanted values.
  cidx, cols = pyp.get_columns_index(df)
  np_data = df.to_numpy()
  holes = np.argwhere(_is_hole(np_data))
  data = np.copy(np_data)
  q = collections.defaultdict(set)
  for i in range(len(holes)):
    x, y = holes[i]
    sym, field = split_field(cols[y])
    if sym:
      q[sym].add(x)

  in_debug = alog.level_active(alog.DEBUG1)
  fill_cols = ('c', 'o', 'h', 'l', 'v')
  dropped_rows = set()
  for s, xset in q.items():
    ci, oi, hi, li, vi = [cidx[f'{s}.{f}'] for f in fill_cols]
    for x in xset:
      c, o, h, l, v = map(lambda j: np_data[x, j], [ci, oi, hi, li, vi])
      if in_debug:
        alog.debug1(f'{s}[{x}] c={c}\to={o}\th={h}\tl={l}\tv={v}')
      if _is_hole(v):
        v = _scan_column(data, x, vi, lambda x: not _is_hole(x))
        if v is None:
          dropped_rows.add(x)
          continue
      # Sometimes rows have Open price but no Close price, and sometimes (though
      # rarely) it is the contrary.
      if _is_hole(c):
        if not _is_hole(o):
          c = o
        else:
          # If Close is zero and there is no Open price, go fetch previous or
          # next Close price in time.
          c = _scan_column(data, x, ci, lambda x: not _is_hole(x))
          if c is None:
            dropped_rows.add(x)
            continue

      if _is_hole(o):
        o = c
      if _is_hole(h):
        h = max(c, o)
      if _is_hole(l):
        l = min(c, o)
      data[x, ci] = c
      data[x, oi] = o
      data[x, hi] = h
      data[x, li] = l
      data[x, vi] = v

  if dropped_rows:
    alog.debug1(f'Dropping {len(dropped_rows)} out of {len(df)}')

    mask = np.full(len(df), True)
    mask[list(dropped_rows)] = False
    data = data[mask]
    index = df.index[mask]
  else:
    index = df.index

  return pd.DataFrame(data=data, index=index, columns=df.columns)


def transform_raw_data(df, start_date=None, end_date=None):
  alog.debug1(f'Reshaping data ...')
  df = reshape_dataframe(df)

  if start_date is not None or end_date is not None:
    alog.debug1(f'Time filtering data ...')
    df = time_filter_dataframe(df, start_date=start_date, end_date=end_date)

  alog.debug1(f'Fixing up missing data ...')
  df = fixup_dataframe(df)

  return df


def invalid_value(dtype):
  return np.nan if np.issubdtype(dtype, np.floating) else 0


def reshape_dataframe(df):
  cols = pyp.get_df_columns(df, discards={'t', 'symbol'})

  is64 = [df[c].dtype == np.float64 for c in cols]
  dtype = np.float64 if any(is64) else np.float32

  groups = pyp.get_dataframe_groups(df, ('t',))
  times = sorted(t for (t,) in groups.keys())
  tidx = pyu.make_index_dict(times)

  symbols = get_df_column_unique(df, 'symbol')
  rcols = [f'{s}.{c}' for s in symbols for c in cols]
  ridx = pyu.make_index_dict(rcols)

  data = np.full((len(times), len(rcols)), invalid_value(dtype), dtype=dtype)

  cdata = tuple((c, df[c]) for c in cols)
  sym_data = df['symbol']

  for (t,), g in groups.items():
    ti = tidx[t]
    for row in g:
      s = sym_data[row]
      for c, d in cdata:
        ci = ridx[f'{s}.{c}']
        data[ti, ci] = d[row]

  alog.debug0(f'Reshaped DataFrame has {len(times)} time points out of {len(df)}')

  return pd.DataFrame(data=data, index=pd.Index(data=times, name='t'), columns=rcols)


def merge_dataframes(dfs, gby_cols=('t', 'symbol')):
  alog.debug0(f'Joining {len(dfs)} DataFrame(s) on {gby_cols}')
  cdf = pd.concat(dfs, ignore_index=True)

  alog.debug0(f'Pruning joined DataFrame with {len(cdf)} rows using {gby_cols}')
  mdf = pyp.limit_per_group(cdf, gby_cols, 1)

  alog.debug0(f'Merged DataFrame has {len(mdf)} time points (concat DataFrame had {len(cdf)})')

  return mdf


def merge_reshaped_dataframes(dfs):
  tdfs = []
  for df in dfs:
    # Input DataFrames have 't' as index, move it into a column with a reset_index().
    tas.check_eq(df.index.name, 't', msg=f'DataFrame must have "t" EPOCH time index')
    tdfs.append(df.reset_index())

  mdf = merge_dataframes(tdfs, gby_cols=('t',))

  return pyp.sorted_index(mdf, 't')


def make_bars_dataframe(bars, dtype=None):
  df = pd.DataFrame(data=bars)
  if dtype is not None:
    if not isinstance(dtype, dict):
      cols = pyp.get_typed_columns(df, pyn.is_numeric, discards={'t'})
      dtype = {c: dtype for c in cols}

    df = pyp.type_convert_dataframe(df, dtype)

  return df


def create_data(api, start_date, end_date, symbols=None, output_file=None,
                data_step='1Min', dtype=np.float32):
  df = api.fetch_data(symbols, start_date, end_date,
                      data_step=data_step,
                      dtype=dtype)
  df = transform_raw_data(df, start_date=start_date, end_date=end_date)
  if output_file is not None:
    alog.debug1(f'Saving data to {output_file} ...')
    pyp.save_dataframe(df, output_file)

  return df


def create_split_data(api, start_date, end_date, symbols, data_step='1Min',
                      dtype=np.float32):
  df = api.fetch_data(symbols, start_date, end_date,
                      data_step=data_step,
                      dtype=dtype)

  dfs = dict()
  for symbol, sdf in df.groupby(['symbol']):
    sdf = sdf.reset_index(drop=True)
    dfs[symbol] = transform_raw_data(sdf, start_date=start_date, end_date=end_date)

  return dfs


_FIELDS = ('o', 'h', 'l', 'c', 'v')

def get_nearest_candel(api, symbols, date):
  candels = dict()

  delta = datetime.timedelta(hours=1)
  data_step = '1Min'
  for i in range(10):
    left_symbols = sorted(set(symbols) - set(candels.keys()))
    if not left_symbols:
      break

    df = api.fetch_data(left_symbols, start_date=date - delta, end_date=date + delta,
                        data_step=data_step)
    if df is not None:
      for symbol, gdf in df.groupby('symbol'):
        field_values = [gdf[n] for n in _FIELDS]
        times = get_dataframe_index_time(gdf)
        index = gdf.index
        tdists = np.abs(times - date.timestamp())
        for x in np.argsort(tdists):
          values = [d[index[x]] for d in field_values]
          if all([z != 0 for z in values]):
            candels[symbol] = {n: v for n, v in zip(_FIELDS, values)}
            break

    delta *= 2
    data_step = '15Min'

  return candels


def save_reshaped_data(df, path, merge=False, dtype=np.float32):
  if not os.path.exists(path) or not merge:
    alog.debug1(f'Saving data ({len(df)} rows) to {path} ...')
    pyp.save_dataframe(df, path)
  else:
    odf = pyp.load_dataframe(path, dtype=dtype)
    df = merge_reshaped_dataframes([odf, df])

    alog.debug1(f'Saving merged data ({len(df)} rows) to {path} ...')
    pyp.save_dataframe(df, path)

  return df


def csv_parse_columns(text):
  pos = text.find('\n')
  if pos > 0:
    headers = text[: pos].strip()
    if headers:
      return [c.strip() for c in headers.split(',')]

  return []


def wait_for_market_open(api):
  tas.check(api.supports_trading, msg=f'{api.name} API does not support trading')

  now = pyd.now()
  market_open, market_close = api.get_market_hours(now)
  if now > market_close:
    market_open, market_close = api.get_market_hours(
      now + datetime.timedelta(days=1))

  if market_open > now:
    wait = market_open - now
    alog.info(f'Market will open at {market_open}, waiting for {wait} ...')
    time.sleep(wait.total_seconds())

  alog.debug0(f'Market is open since {market_open}')

  return market_open, market_close

