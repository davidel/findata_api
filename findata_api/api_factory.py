import argparse
import collections
import copy
import importlib
import os
import threading

from py_misc_utils import alog
from py_misc_utils import cleanups
from py_misc_utils import dyn_modules as pydm
from py_misc_utils import utils as pyu


def _detect_apis():
  parent, _ = pyu.split_module_name(__name__)

  apis = pydm.DynLoader(parent, '_api')
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


_LOCK = threading.Lock()
_API_CACHE = dict()

@cleanups.reg
def _cleanup():
  with _LOCK:
    apis = list(_API_CACHE.values())
    _API_CACHE.clear()

    for api in apis:
      api.close()


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
    with _LOCK:
      api = _API_CACHE.get(name, None)
      if api is None:
        api = mod.create_api(_ARGS)
        _API_CACHE[name] = api

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

