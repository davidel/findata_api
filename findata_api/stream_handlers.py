import threading


class StreamHandlers:

  TRADE = '_trade_handlers'
  QUOTE = '_quote_handlers'
  BAR = '_bar_handlers'

  HANDLERS = (TRADE, QUOTE, BAR)

  def __init__(self):
    self._lock = threading.Lock()
    for hname in self.HANDLERS:
      setattr(self, hname, tuple())

  def get_handlers(self):
    handlers = dict()
    with self._lock:
      for hn in self.HANDLERS:
        handlers[hn] = getattr(self, hname)

    return handlers

  def _run_handlers(self, hname, x):
    handlers = getattr(self, hname)

    for handler in handlers:
      handler(x)

  def _trade_handler(self, x):
    self._run_handlers(self.TRADE, x)

  def _quote_handler(self, x):
    self._run_handlers(self.QUOTE, x)

  def _bar_handler(self, x):
    self._run_handlers(self.BAR, x)

  def _add_handler(self, hname, handler):
    with self._lock:
      handlers = list(getattr(self, hname))
      handlers.append(handler)
      setattr(self, hname, tuple(handlers))

  def _remove_handler(self, hname, handler):
    with self._lock:
      handlers = list(getattr(self, hname))
      try:
        handlers.remove(handler)
        setattr(self, hname, tuple(handlers))
      except ValueError:
        handler = None

    return handler

  def add_trade_handler(self, handler):
    self._add_handler(self.TRADE, handler)

  def add_quote_handler(self, handler):
    self._add_handler(self.QUOTE, handler)

  def add_bar_handler(self, handler):
    self._add_handler(self.BAR, handler)

  def remove_trade_handler(self, handler):
    self._remove_handler(self.TRADE, handler)

  def remove_quote_handler(self, handler):
    self._remove_handler(self.QUOTE, handler)

  def remove_bar_handler(self, handler):
    self._remove_handler(self.BAR, handler)

