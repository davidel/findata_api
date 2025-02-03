import argparse
import collections
import copy
import importlib
import os
import threading

import py_misc_utils.alog as alog
import py_misc_utils.cleanups as cleanups
import py_misc_utils.dyn_modules as pydm
import py_misc_utils.global_namespace as gns
import py_misc_utils.module_utils as pymu
import py_misc_utils.utils as pyu


def _detect_apis():
  parent, _ = pymu.split_module_name(__name__)

  apis = pydm.DynLoader(modname=parent, postfix='_api')
  module_names = apis.module_names()

  order = {name: len(module_names) - i for i, name in enumerate(os.getenv(
    'FINDATA_API_ORDER',
    'finnhub,yfinance,polygon,alpha_vantage,alpaca').split(','))}

  ordered_modules = sorted(module_names,
                           key=lambda x: order.get(x, -1),
                           reverse=True)

  return apis, tuple(ordered_modules)


_APIS, _API_NAMES = _detect_apis()
_ARGS = None

def setup_api(args):
  global _ARGS
  _ARGS = args


def add_api_options(parser):
  parser.add_argument('--api', type=str,
                      choices=_API_NAMES,
                      help='The API to use')
  parser.add_argument('--api_rate', type=float,
                      default=pyu.getenv('API_RATE', dtype=float),
                      help='The maximum number of API calls per minute')

  for mod in _APIS.modules():
    mod.add_api_options(parser)


class _ApiCache:

  def __init__(self):
    self._lock = threading.Lock()
    self._cache = dict()
    self._cid = cleanups.register(self.clear)

  def get(self, mod, name, args):
    with self._lock:
      api = self._cache.get(name)
      if api is None:
        api = mod.create_api(args)
        self._cache[name] = api

  def clear(self):
    with self._lock:
      apis = list(self._cache.values())
      self._cache = dict()

    for api in apis:
      api.close()


_API_CACHE = gns.Var(f'{__name__}.API_CACHE',
                     fork_init=True,
                     defval=lambda: _ApiCache())

def _api_cache():
  return gns.get(_API_CACHE)


def _merged_args(sargs, nargs):
  if nargs:
    args = copy.copy(sargs)
    for k, v in nargs.items():
      setattr(args, k, v)

    return args

  return sargs


def create_api(name=None, create=False, args=None):
  if name is None:
    name = _ARGS.api or _API_NAMES[0]
  mod = _APIS.get(name)
  if mod is None:
    alog.xraise(RuntimeError, f'Invalid API name: {name}')

  if create:
    api = mod.create_api(_merged_args(_ARGS, args))
  else:
    api = _api_cache().get(mod, name, _ARGS)

  alog.debug0(f'Using {api.name} API')

  return api


def select_api(start_date, end_date, data_step):
  # In order of preference.
  api_kinds = list(_API_NAMES)
  # First try with the eventually specified API.
  if _ARGS.api:
    api = create_api(name=_ARGS.api)
    if api.range_supported(start_date, end_date, data_step):
      return api
    api_kinds.remove(_ARGS.api)

  for kind in api_kinds:
    api = create_api(name=kind)
    if api.range_supported(start_date, end_date, data_step):
      return api

  alog.xraise(RuntimeError, f'Unable to select valid API: start={start_date}\t' \
              f'end={end_date}\tstep={data_step}')

