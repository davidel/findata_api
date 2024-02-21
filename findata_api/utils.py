import array
import bisect
import collections
import datetime
import math
import re

import numpy as np
import pandas as pd
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import np_utils as pyn
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu


class MarketTimeFilter:

  def __init__(self, open_delta=0, close_delta=0, tz=None):
    # Market opens at 09:30 and closes at 16:00 US Eastern time.
    # 33250 = 09:30 seconds from midnight.
    # 57600 = 16:00 seconds from midnight.
    self._open_offset = 33250 + open_delta
    self._close_offset = 57600 + close_delta
    self._tz = tz
    self._day_starts = [0]
    self._last_ds = 0

  def _add_entry(self, t, pos):
    ts = pyd.from_timestamp(t, tz=self._tz)
    dts = ts.replace(hour=0, minute=0, second=0, microsecond=0)
    ds = dts.timestamp()
    self._day_starts.insert(pos, ds)

    return ds

  def _get_ds(self, t):
    # 86400 = Seconds per day.
    if 0 <= (t - self._last_ds) < 86400:
      return self._last_ds

    pos = bisect.bisect_right(self._day_starts, t) - 1
    ds = self._day_starts[pos]
    if t - ds >= 86400:
      ds = self._add_entry(t, pos + 1)
    self._last_ds = ds

    return ds

  def filter(self, t):
    ds = self._get_ds(t)

    return self._open_offset <= (t - ds) <= self._close_offset

  def filter2(self, t):
    ds = self._get_ds(t)

    return self._open_offset <= (t - ds) <= self._close_offset, ds


def market_filter(df, epoch_col, open_delta=0, close_delta=0, tz=None):
  mkf = MarketTimeFilter(open_delta=open_delta, close_delta=close_delta, tz=tz)

  alog.debug0(f'Market-filtering {len(df)} records ...')

  mask = [mkf.filter(t) for t in pyp.column_or_index(df, epoch_col)]
  mdf = df[mask]
  if mdf.index.name is None:
    mdf = mdf.reset_index(drop=True)

  alog.info(f'Market-filtered {len(mdf)} out of {len(df)}')

  return mdf


def day_split_exec(df, epoch_col, exec_fn, exec_args=None,
                   open_delta=0, close_delta=0, tz=None, max_gap=None):
  tz = tz or pyd.us_eastern_timezone()
  mkf = MarketTimeFilter(open_delta=open_delta, close_delta=close_delta, tz=tz)

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
    alog.debug0(f'Split has {len(indices)} rows at {pyd.from_timestamp(t, tz=tz).isoformat()}')

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
  return sorted(split_columns(df.columns.tolist()).keys())


def get_data_step_delta(data_step):
  if isinstance(data_step, datetime.timedelta):
    return data_step

  m = re.match(r'(\d+(\.\d+)?)?([a-z]+)', data_step.lower())
  if not m:
    alog.xraise(RuntimeError, f'Invalid data step: {data_step}')
  count, unit = m.group(1), m.group(3)
  n = float(count) if count else 1
  if unit in ('s', 'sec', 'second'):
    return datetime.timedelta(seconds=n)
  if unit in ('m', 'min', 'minute'):
    return datetime.timedelta(minutes=n)
  if unit in ('h', 'hour'):
    return datetime.timedelta(hours=n)
  if unit in ('d', 'day'):
    return datetime.timedelta(days=n)
  if unit in ('w', 'wk', 'week'):
    return datetime.timedelta(days=n * 7)
  if unit in ('mo', 'month'):
    return datetime.timedelta(days=n * 30)

  alog.xraise(RuntimeError, f'Invalid data step: {data_step}')


def infer_time_range(start_date, end_date, data_step, limit=None, tz=None):
  limit = limit or 10
  step_delta = get_data_step_delta(data_step) * limit

  if not end_date:
    if start_date:
      end_date = start_date + step_delta
      tnow = pyd.now(tz=start_date.tzinfo)
      if end_date > tnow:
        end_date = tnow
    else:
      end_date = pyd.now(tz=tz)

  if not start_date:
    start_date = end_date - step_delta

  return start_date, end_date


def break_period_in_dates_list(start_date, end_date, data_step, limit):
  step_delta = get_data_step_delta(data_step)
  delta = step_delta * limit
  step_start_date = start_date
  dates_list = []
  while end_date > (step_start_date + delta):
    dates_list.append(pyu.make_object(start=step_start_date,
                                      end=step_start_date + delta,
                                      limit=limit))
    step_start_date += delta

  end_limit = int((end_date - step_start_date) / step_delta) + 1
  dates_list.append(pyu.make_object(start=step_start_date, end=end_date,
                                    limit=end_limit))

  return dates_list


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


def purge_fetched_data(df, start_date, end_date, limit, data_step):
  alog.debug0(f'Sorting by time')
  df = df.sort_values('t', ignore_index=True)

  if start_date or end_date:
    alog.debug0(f'Time filtering fetched data')
    df = time_filter_dataframe(df, start_date=start_date, end_date=end_date, col='t')
  if limit is not None:
    if start_date:
      if not end_date:
        df = pyp.limit_per_group(df, ['symbol'], limit)
    else:
      df = pyp.limit_per_group(df, ['symbol'], -limit)

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
  if isinstance(values.dtype, object):
    if isinstance(values[0], str):
      values = pd.to_datetime(values)

    # I am not sure if a feature or a bug, but when calling a Pandas Series
    # to_numpy() it sometimes returns an array of `object` dtype and pd.Timestamp
    # values, instead of correctly converting the latter to np.datetime64 (like a
    # pd.Timestamp.to_numpy() would).
    # We do it here anywhere since we really want np.datetime64.
    if hasattr(values[0], 'to_numpy'):
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
  for i in range(0, len(holes)):
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

  if start_date or end_date:
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
  times = sorted([t for (t,) in groups.keys()])
  tidx = pyu.make_index_dict(times)

  symbols = get_df_column_unique(df, 'symbol')
  rcols = [f'{s}.{c}' for s in symbols for c in cols]
  ridx = pyu.make_index_dict(rcols)

  data = np.full((len(times), len(rcols)), invalid_value(dtype), dtype=dtype)

  cdata = tuple([(c, df[c]) for c in cols])
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


def get_nearest_candel(api, symbols, date):
  candels = dict()

  fields = ('o', 'h', 'l', 'c', 'v')

  delta = datetime.timedelta(hours=1)
  data_step = '1Min'
  for i in range(0, 10):
    left_symbols = sorted(set(symbols) - set(candels.keys()))
    if not left_symbols:
      break

    df = api.fetch_data(left_symbols, start_date=date - delta, end_date=date + delta,
                        data_step=data_step)
    if df is not None and len(df) > 0:
      for symbol, gdf in df.groupby('symbol'):
        o, h, l, c, v = [gdf[n] for n in fields]
        times = get_dataframe_index_time(gdf)
        index = gdf.index
        tdists = np.abs(times - date.timestamp())
        for x in np.argsort(tdists):
          values = [d[index[x]] for d in (o, h, l, c, v)]
          if all([z != 0 for z in values]):
            candels[symbol] = {n: v for n, v in zip(fields, values)}
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

