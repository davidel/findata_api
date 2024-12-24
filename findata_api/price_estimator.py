import array
import collections
import math
import os
import re

import numpy as np
import pandas as pd
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.core_utils as pycu
import py_misc_utils.date_utils as pyd
import py_misc_utils.pd_utils as pyp
import py_misc_utils.named_array as nar
import py_misc_utils.utils as pyu


_TRADES_FIELDS = (
  pyu.make_object(name='timestamp', fmt='d'),
  pyu.make_object(name='quantity', fmt='f'),
  pyu.make_object(name='price', fmt='f'),
)
_CONFIG_FILE = 'config.yaml'
_TRADES_DIR = 'trades'
_TRADES_IDX_DIR = 'trades_indices'


def _defval(v, dv):
  return v if v is not None else dv


def _obj_load(obj, rtype):
  return obj if isinstance(obj, rtype) else obj()


class Storage:

  def __init__(self, fields):
    self.fields = fields
    names = [f.name for f in fields]
    fmt = ''.join([f.fmt for f in fields])
    self._symbols = collections.defaultdict(lambda: nar.NamedArray(names, fmt=fmt))

  def append(self, sym, *args):
    self._symbols[sym].append(*args)

  def append_extend(self, sym, *args):
    self._symbols[sym].append_extend(*args)

  def to_numpy(self):
    stg = dict()
    for sym, na in self._symbols.items():
      sym_data = dict()
      for field, data in na.data.items():
        sym_data[field] = np.array(data)

      stg[sym] = sym_data

    return stg


class Estimator:

  def __init__(self, trades_stg=None, trades_window=None, trades_time_scaler=None,
               trades_indices=None):
    self._trades_stg = trades_stg
    self._trades_window = _defval(trades_window, 6)
    self._trades_time_scaler = _defval(trades_time_scaler, 25.0)
    self._trades_indices = trades_indices if trades_indices else _make_indices(trades_stg)

  def _trades_data(self, sym, *args):
    sym_data = self._trades_stg[sym]
    return [sym_data[x] for x in args]

  @property
  def symbols(self):
    return sorted(self._trades_stg.keys())

  def describe(self):
    dlines = [f'Price estimator information:']
    for sym in self._trades_stg.keys():
      timestamps, *_ = self._trades_data(sym, 'timestamp')
      indices = self._trades_indices[sym]

      tstart = pyd.from_timestamp(timestamps[indices[0]]).isoformat()
      tend = pyd.from_timestamp(timestamps[indices[-1]]).isoformat()
      dlines.append(f'  {sym}\ttstart={tstart}\ttend={tend}\tsamples={len(indices)}')

    return '\n'.join(dlines)

  def create_trades_storage(self, symbols=None):
    syms = set(self._trades_stg.keys())
    if symbols:
      syms = syms & set(symbols)
    field_names = [f.name for f in _TRADES_FIELDS]

    stg = Storage(_TRADES_FIELDS)
    for sym in syms:
      tdata = self._trades_data(sym, *field_names)
      stg.append_extend(sym, *tdata)

    return stg

  def get_price(self, sym, ts):
    timestamps, quantities, prices = self._trades_data(sym, 'timestamp', 'quantity', 'price')

    indices = self._trades_indices[sym]
    pos = pycu.bisect_right(ts, lambda i: timestamps[indices[i]], len(indices)) - 1

    tbase = timestamps[indices[pos]]
    scaler, total = 0, 0
    for i in range(pos, max(pos - self._trades_window, 0) - 1, -1):
      x = indices[i]
      dt = tbase - timestamps[x]

      weight = quantities[x] * math.exp(-dt / self._trades_time_scaler)
      scaler += weight
      total += prices[x] * weight

    return total / scaler if scaler != 0 else None

  def estimate_gain(self, sym, t1, t2, scale=100.0):
    p1, p2 = self.get_price(sym, t1), self.get_price(sym, t2)
    if p1 is not None and p2 is not None:
      return scale * (p2 - p1) / p1

  def get_trades_index_range(self, sym, t0, t1):
    timestamps, *_ = self._trades_data(sym, 'timestamp')

    indices = self._trades_indices[sym]
    pos = pycu.bisect_right(t0, lambda i: timestamps[indices[i]], len(indices)) - 1

    for epos in range(pos, len(indices)):
      x = indices[epos]
      if timestamps[x] >= t1:
        break

    return pos, epos

  def get_trades(self, sym, irange, fields):
    indices = self._trades_indices[sym]
    xindices = indices[irange[0]: irange[1]]

    data = self._trades_data(sym, *fields)

    return [td[xindices] for td in data]

  def configure(self, trades_window=None, trades_time_scaler=None):
    if trades_window is not None:
      alog.debug0(f'Setting trades_window={trades_window}')
      self._trades_window = trades_window
    if trades_time_scaler is not None:
      alog.debug0(f'Setting trades_time_scaler={trades_time_scaler}')
      self._trades_time_scaler = trades_time_scaler

  def save(self, path):
    os.mkdir(path)
    _save_numpy_storage(path, _TRADES_DIR, self._trades_stg)

    conf = dict(trades_window=self._trades_window,
                trades_time_scaler=self._trades_time_scaler)
    pyu.write_config(conf, os.path.join(path, _CONFIG_FILE))

    _save_indices(path, _TRADES_IDX_DIR, self._trades_indices)

  @staticmethod
  def _load_config(path):
    conf_path = os.path.join(path, _CONFIG_FILE)
    if os.path.exists(conf_path):
      return pyu.load_config(conf_path)

    return dict()

  @staticmethod
  def load(path):
    trades_stg = _load_numpy_storage(path, _TRADES_DIR)
    trades_indices = _load_indices(path, _TRADES_IDX_DIR)
    conf = Estimator._load_config(path)

    return Estimator(trades_stg=trades_stg,
                     trades_window=conf.get('trades_window'),
                     trades_time_scaler=conf.get('trades_time_scaler'),
                     trades_indices=trades_indices)


def _save_numpy_storage(path, name, stg):
  rpath = os.path.join(path, name)
  os.mkdir(rpath)
  for sym, sym_data in stg.items():
    spath = os.path.join(rpath, sym)
    os.mkdir(spath)
    for n, t in sym_data.items():
      tpath = os.path.join(spath, f'{n}.npy')
      np.save(tpath, t)


def _load_numpy_storage(path, name):
  stg = dict()
  rpath = os.path.join(path, name)
  for sym in os.listdir(rpath):
    spath = os.path.join(rpath, sym)
    if os.path.isdir(spath):
      sym_data = dict()
      for tname in os.listdir(spath):
        name, ext = os.path.splitext(tname)
        tas.check_eq(ext, '.npy')

        tpath = os.path.join(spath, tname)
        sym_data[name] = np.lib.format.open_memmap(tpath, mode='r')

      stg[sym] = sym_data

  return stg


def _save_indices(path, name, indices):
  ipath = os.path.join(path, name)
  os.mkdir(ipath)
  for sym, idx in indices.items():
    np.save(os.path.join(ipath, f'{sym}.npy'), idx)


def _load_indices(path, name):
  indices = dict()
  ipath = os.path.join(path, name)
  for sname in os.listdir(ipath):
    sym, ext = os.path.splitext(sname)
    tas.check_eq(ext, '.npy')

    spath = os.path.join(ipath, sname)
    indices[sym] = np.lib.format.open_memmap(spath, mode='r')

  return indices


def _storage_append(dfs, stg, symbols=None):
  syms = set(symbols) if symbols else None
  for df in dfs:
    # We allow passing in loader functions, so if the type is not a Pandas
    # DataFrame, it must be a loader function.
    df = _obj_load(df, pd.DataFrame)

    cidx, _ = pyp.get_columns_index(df)
    rmap = [cidx[f.name] for f in stg.fields]
    si = cidx['symbol']

    for rec in df.itertuples(name=None, index=False):
      sym = rec[si]
      if syms is None or sym in syms:
        args = [rec[rmap[i]] for i in range(len(rmap))]
        stg.append(sym, *args)

  return stg.to_numpy()


def _make_indices(stg, ts_field='timestamp'):
  sidx = dict()
  for sym, sym_data in stg.items():
    sidx[sym] = np.argsort(sym_data[ts_field])

  return sidx


def _get_estimator_storages(estimator_path, symbols=None):
  alog.debug0(f'Loading source estimator data from {estimator_path}')
  est = Estimator.load(estimator_path)

  return pyu.make_object(trades=est.create_trades_storage(symbols=symbols),
                         quotes=None)


def create_from_dataframe(trades_df=None,
                          trades_loaders=None,
                          estimator_path=None,
                          symbols=None,
                          trades_window=None,
                          trades_time_scaler=None):
  if estimator_path:
    est_storages = _get_estimator_storages(estimator_path, symbols=symbols)
    trades_stg = est_storages.trades
  else:
    trades_stg = Storage(_TRADES_FIELDS)
  dfs = trades_loaders if trades_df is None else [trades_df]
  trades_stg = _storage_append(dfs, trades_stg, symbols=symbols)

  return Estimator(trades_stg=trades_stg,
                   trades_window=trades_window,
                   trades_time_scaler=trades_time_scaler)

