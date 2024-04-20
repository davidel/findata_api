import numpy as np
from py_misc_utils import alog
from py_misc_utils import assert_checks as tas
from py_misc_utils import date_utils as pyd
from py_misc_utils import stream_dataframe as stdf
from py_misc_utils import utils as pyu

from . import market_hours as mh
from . import splits as spl


def _load_fields_map(fmstr):
  fmap = dict()
  for ff in pyu.comma_split(fmstr):
    parts = pyu.resplit(ff, '=')
    if len(parts) == 1:
      fmap[parts[0]] = parts[0]
    else:
      fmap[parts[0]] = parts[1]

  return fmap


class BarsResampler:

  _STD_FIELDS_MAP = 't,symbol,o,h,l,c,v'

  def __init__(self, reader, interval,
               buffer_size=None,
               fields_map=None,
               splits_path=None):
    self._reader = reader
    self._interval = interval
    self._buffer_size = buffer_size or 10000
    self._fmap = _load_fields_map(fields_map or self._STD_FIELDS_MAP)
    self._splits = spl.Splits(splits_path) if splits_path is not None else None
    self._wdata = self._reader.empty_array(self._buffer_size)
    self._time_scan = stdf.StreamSortedScan(reader, self._fmap['t'])
    self._init()

  def _init(self):
    self._widx = 0
    self._btime = 0
    self._symbols = dict()

  def _write(self, sdwriter, wdata):
    # Data handed over to the stdf.StreamDataWriter() will be owned by it.
    # Since we also want to keep the buffers, we make a copy.
    cpdata = dict()
    for field, data in wdata.items():
      if isinstance(data, np.ndarray):
        cpdata[field] = np.copy(data)
      else:
        cpdata[field] = np.array(data)

    sdwriter.write(**cpdata)

  def _write_data(self, sdwriter, force=False):
    if self._widx == self._buffer_size:
      self._write(sdwriter, self._wdata)
      self._widx = 0
    elif force and self._widx:
      if self._widx < self._buffer_size:
        wdata = {field: data[: self._widx] for field, data in self._wdata.items()}
      else:
        wdata = self._wdata

      self._write(sdwriter, wdata)
      self._widx = 0

  def _splits_rewrite(self, se):
    if self._splits is not None:
      factor = self._splits.factor(se.symbol, pyd.from_timestamp(se.t))
      se.c *= factor
      se.h *= factor
      se.l *= factor
      se.o *= factor
      se.v /= factor

    return se

  def _write_symbol(self, se, sdwriter):
    self._write_data(sdwriter)

    se = self._splits_rewrite(se)

    widx = self._widx
    for field, data in self._wdata.items():
      data[widx] = getattr(se, field)

    self._widx += 1

  def _flush_symbols(self, sdwriter):
    for se in self._symbols.values():
      self._write_symbol(se, sdwriter)

    self._symbols = dict()

  def _bar_time(self, t):
    return ((int(t) + self._interval) // self._interval) * self._interval

  def _feed(self, sdwriter, t, symbol, o, h, l, c, v):
    if t >= self._btime:
      self._flush_symbols(sdwriter)
      self._btime = self._bar_time(t)

    se = self._symbols.get(symbol)
    if se is None:
      se = pyu.make_object(t=self._bar_time(t),
                           symbol=symbol,
                           o=o,
                           h=h,
                           l=l,
                           c=c,
                           v=v)
      self._symbols[symbol] = se
    else:
      se.h = max(se.h, h)
      se.l = min(se.l, l)
      se.c = c
      se.v += v

  def _load_data(self, rdata, *args):
    data = []
    for ff in args:
      fm = self._fmap[ff]
      data.append(rdata[fm])

    return tuple(data)

  def resample(self, path):
    sdwriter = stdf.StreamDataWriter(self._reader.typed_fields(), path)
    mkf = mh.MarketTimeTracker()

    self._init()

    nrecs, nproc = len(self._reader), 0
    for size, rdata in self._time_scan.scan():
      t, symbol, o, h, l, c, v = self._load_data(rdata, 't', 'symbol', 'o',
                                                 'h', 'l', 'c', 'v')

      for i in range(size):
        if mkf.filter(t[i]):
          self._feed(sdwriter, t[i], symbol[i], o[i], h[i], l[i], c[i], v[i])

      nproc += size
      alog.info(f'Processed {nproc}/{nrecs} ({100 * nproc / nrecs:.1f}%)')

    self._flush_symbols(sdwriter)
    self._write_data(sdwriter, force=True)

    sdwriter.flush()

