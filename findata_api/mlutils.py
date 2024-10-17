import numpy as np
import py_misc_utils.alog as alog
import py_misc_utils.assert_checks as tas
import py_misc_utils.date_utils as pyd
import py_misc_utils.np_utils as pyn
import py_misc_utils.utils as pyu


def get_interval_indices(dim_size, size, base=None):
  tas.check_ge(dim_size, 0)
  tas.check_ge(size, 0)
  tas.check_le(size, 1)

  base = base if base is not None else (1.0 - size)
  tas.check_ge(base, 0)
  tas.check_le(base, 1)
  tas.check_le(base + size, 1)

  return int(base * dim_size), int((base + size) * dim_size)


def get_training_indices(size, times=None, test_start_date=None, test_end_date=None,
                         test_size=None, test_base=None, separation=None):
  reindex = None
  if test_size is not None and test_base is not None:
    base, end = get_interval_indices(size, test_size, base=test_base)

    test_indices = np.arange(base, end)
  elif test_start_date is not None and test_end_date is not None:
    if times is None:
      alog.xraise(RuntimeError, f'Time based test indices selected, but inputs have no time information')
    if isinstance(test_start_date, str):
      test_start_date = pyd.parse_date(test_start_date, tz=pyd.ny_market_timezone())
    if isinstance(test_end_date, str):
      test_end_date = pyd.parse_date(test_end_date, tz=pyd.ny_market_timezone())
    tstart = test_start_date.timestamp()
    tend = test_end_date.timestamp()

    reindex = np.argsort(times)
    stimes = times[reindex]

    test_indices = np.flatnonzero((stimes >= tstart) & (stimes < tend))
  else:
    alog.xraise(RuntimeError, f'Missing arguments to compute training indices')

  train_indices = pyn.complement_indices(test_indices, size)

  if separation is not None:
    tmin = np.amin(test_indices) - separation
    tmax = np.amax(test_indices) + separation
    train_indices = train_indices[(train_indices < tmin) | (train_indices > tmax)]

  if reindex is not None:
    train_indices = np.sort(reindex[train_indices])
    test_indices = np.sort(reindex[test_indices])

  alog.debug1(f'TrainSize = {len(train_indices)}\tTestSize = {len(test_indices)}')

  return pyu.make_object(train=train_indices, test=test_indices, size=size)

