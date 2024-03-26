import threading

from . import stream_handlers as sth


class StreamBroadcast(sth.StreamHandlers):

  def __init__(self, api, symbols):
    super().__init__()
    self._api = api
    self._symbols = sorted(symbols)
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

  def _trade_handler(self, x):
    self._run_handlers(self.TRADE, x)

  def _quote_handler(self, x):
    self._run_handlers(self.QUOTE, x)

  def _bar_handler(self, x):
    self._run_handlers(self.BAR, x)

