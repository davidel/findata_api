import threading


class StreamBroadcast:

  def __init__(self, api, symbols):
    self._api = api
    self._symbols = sorted(symbols)
    self._lock = threading.Lock()
    self._trade_handlers = tuple()
    self._quote_handlers = tuple()
    self._bar_handlers = tuple()
    self._stream_handlers = dict(quotes=self._quote_handler,
                                 trades=self._trade_handler,
                                 bars=self._bar_handler)
    self._started = 0

  def start(self):
    with self._lock:
      started = self._started
      self._started += 1

    if started == 0:
      self._api.register_stream_handlers(self._symbols, self._stream_handlers)

  def stop(self):
    with self._lock:
      started = self._started
      self._started -= 1

    if started == 1:
      self._api.register_stream_handlers([], dict())

  def _run_handlers(self, aname, x):
    handlers = getattr(self, aname)

    for handler in handlers:
      handler(x)

  def _trade_handler(self, x):
    self._run_handlers('_trade_handlers', x)

  def _quote_handler(self, x):
    self._run_handlers('_quote_handlers', x)

  def _bar_handler(self, x):
    self._run_handlers('_bar_handlers', x)

  def _add_handler(self, aname, handler):
    with self._lock:
      handlers = list(getattr(self, aname))
      handlers.append(handler)
      setattr(self, aname, tuple(handlers))

  def _remove_handler(self, aname, handler):
    with self._lock:
      handlers = list(getattr(self, aname))
      try:
        handlers.remove(handler)
        setattr(self, aname, tuple(handlers))
      except ValueError:
        handler = None

    return handler

  def add_trade_handler(self, handler):
    self._add_handler('_trade_handlers', handler)

  def add_quote_handler(self, handler):
    self._add_handler('_quote_handlers', handler)

  def add_bar_handler(self, handler):
    self._add_handler('_bar_handlers', handler)

  def remove_trade_handler(self, handler):
    self._remove_handler('_trade_handlers', handler)

  def remove_quote_handler(self, handler):
    self._remove_handler('_quote_handlers', handler)

  def remove_bar_handler(self, handler):
    self._remove_handler('_bar_handlers', handler)

