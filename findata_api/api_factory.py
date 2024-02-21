import argparse
import collections
import os
import threading

from py_misc_utils import alog
from py_misc_utils import cleanups
from py_misc_utils import utils as pyu

from . import ap_api
from . import av_api
from . import fh_api
from . import py_api
from . import yf_api


def _detect_apis():
  apis = collections.OrderedDict()
  # In order of preference in case not user specified with --api.
  for aid in ('fh', 'yf', 'py', 'av', 'ap'):
    mod = globals().get(f'{aid}_api', None)
    if mod is not None:
      name = getattr(mod, 'API_NAME')
      if name is not None:
        apis[name] = mod

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
    api = _APIS[kind].create_api(_ARGS)
    if api.range_supported(start_date, end_date, data_step):
      return api

  alog.xraise(RuntimeError, f'Unable to select valid API: start={start_date}\tend={end_date}\tstep={data_step}')

