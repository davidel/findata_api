import argparse
import collections
import importlib
import os
import re
import threading

from py_misc_utils import alog
from py_misc_utils import cleanups
from py_misc_utils import utils as pyu


def _get_available_modules():
  modules = []
  for fname in os.listdir(os.path.dirname(__file__)):
    # To add a new API implement $API + '_api.py' module within this folder.
    m = re.match(r'(.*)_api\.py$', fname)
    if m:
      modules.append(m.group(1))

  order = {name: len(modules) - i for i, name in enumerate(os.getenv(
    'FINDATA_API_ORDER',
    'finnhub,yfinance,polygon,alpha_vantage,alpaca').split(','))}

  return sorted(modules, key=lambda x: order.get(x, -1), reverse=True)


def _detect_apis():
  parent, _ = pyu.split_module_name(__name__)

  apis = collections.OrderedDict()
  for mod_name in _get_available_modules():
    mod = importlib.import_module(f'{parent}.{mod_name}_api')
    mname = getattr(mod, 'MODULE_NAME', None)
    if mname is not None:
      apis[mname] = mod

  return apis


_APIS = _detect_apis()
_ARGS = None

def setup_api(args):
  global _ARGS
  _ARGS = args


def add_api_options(parser):
  parser.add_argument('--api', type=str,
                      choices=tuple(_APIS.keys()),
                      help='The API to use')
  parser.add_argument('--api_rate', type=float,
                      default=pyu.getenv('API_RATE', dtype=float),
                      help='The maximum number of API calls per minute')

  for mod in _APIS.values():
    mod.add_api_options(parser)


_LOCK = threading.Lock()
_API_CACHE = dict()

@cleanups.register
def _cleanup():
  with _LOCK:
    _API_CACHE.clear()


def create_api(name=None, create=False):
  if name is None:
    name = _ARGS.api or next(iter(_APIS.keys()))
  mod = _APIS.get(name, None)
  if mod is None:
    alog.xraise(RuntimeError, f'Invalid API name: {name}')

  if create:
    api = mod.create_api(_ARGS)
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
  api_kinds = list(_APIS.keys())
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

  alog.xraise(RuntimeError, f'Unable to select valid API: start={start_date}\tend={end_date}\tstep={data_step}')

