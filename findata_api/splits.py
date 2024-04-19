import collections
import bisect

import numpy as np
import pandas as pd
from py_misc_utils import date_utils as pyd
from py_misc_utils import np_utils as pyn
from py_misc_utils import pd_utils as pyp
from py_misc_utils import utils as pyu


SplitEntry = collections.namedtuple('SplitEntry', 'timestamp, split_factor')


def _load_splits_data(path):
  sdata = pyp.load_dataframe_as_npdict(path)

  tz = pyd.ny_market_timezone()

  usym = pyu.unique(sdata['symbol'])

  dates = sdata['date']
  noms = sdata.get('nom')
  dens = sdata.get('den')
  facs = sdata.get('fac')

  splits = collections.defaultdict(list)
  for sym, symidx in usym.items():
    slist = splits[sym]

    for idx in symidx:
      timestamp = pyd.parse_date(dates[idx], tz=tz).timestamp()
      if facs is not None:
        split_factor = facs[idx]
      else:
        split_factor = noms[idx] / dens[idx]

      slist.append(SplitEntry(timestamp=int(timestamp),
                              split_factor=split_factor))

    slist.sort(key=lambda e: e.timestamp)

  return splits


class SequencerBase:

  def factor(self, dt):
    return 1.0


class Sequencer(SequencerBase):

  def __init__(self, slist):
    super().__init__()
    self._slist = slist
    self._timestamp = 0
    self._idx, self._factor = 0, np.prod([e.split_factor for e in slist])

  def factor(self, dt):
    timestamp = int(dt.timestamp())
    if self._timestamp > timestamp:
      return self._factor

    for idx in range(self._idx, len(self._slist)):
      itimestamp = self._slist[idx].timestamp
      if itimestamp > timestamp:
        self._timestamp = itimestamp
        break

      self._factor /= self._slist[idx].split_factor

    self._idx = idx

    return self._factor


class Splits:

  def __init__(self, path):
    # Input DataFrame must have a "date" and "symbol" columns, and then,
    # either a "fac" column expressing the split factor directly, or both
    # a "nom" and "den" columns representing the nominator and denominator
    # of the split factor.
    self._splits = _load_splits_data(path)

  def has_splits(self, symbol):
    return self._splits.get(symbol) is not None

  def factor(self, symbol, dt):
    slist, factor = self._splits.get(symbol), 1.0
    if slist is not None:
      timestamp = int(dt.timestamp())

      idx = bisect.bisect(slist, timestamp, key=lambda e: e.timestamp)
      for e in slist[idx: ]:
        factor *= e.split_factor

    return factor

  # The sequencer() API returns a Sequencer() object whose factor() API must
  # be used with monotonically increasing timestamps in order to work.
  # This is faster than calling the Splits.factor() API, but has such constraint
  # to be complied with. One particular use of this API is during dataset
  # transformations, where one can scan records ordered by timestamp and apply
  # the resulting factors to unadjusted prices.
  def sequencer(self, symbol):
    slist = self._splits.get(symbol)

    return Sequencer(tuple(slist)) if slist else SequencerBase()

  def rewrite_data(self, data, *cols, timecol=None, symcol=None):
    if isinstance(data, pd.DataFrame):
      rwdata = pyp.to_npdict(data)
    else:
      rwdata = pyn.npdict_clone(data)

    timecol = timecol or 't'
    symcol = symcol or 'symbol'

    times = rwdata[timecol]

    usym = pyu.unique(rwdata[symcol])
    for sym, symidx in usym.items():
      if self.has_splits(sym):
        for idx in symidx:
          dt = pyd.from_timestamp(times[idx])
          factor = self.factor(sym, dt)
          if factor != 1.0:
            for c in cols:
              rwdata[c][idx] *= factor

    return pd.DataFrame(data=rwdata, copy=False) if isinstance(data, pd.DataFrame) else rwdata

