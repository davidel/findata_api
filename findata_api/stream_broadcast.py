from . import stream_handlers as sth


class StreamBroadcast(sth.StreamHandlers):

  def __init__(self, api, symbols):
    super().__init__()
    self._api = api
    self._symbols = sorted(symbols)
    self._started = 0

  def start(self):
    with self._lock:
      started = self._started
      handlers = self.get_handlers() if started == 0 else None
      self._started += 1

    if started == 0:
      stream_handlers = dict()
      for hname, hlist in handlers.items():
        if hlist:
          if hname == sth.StreamHandlers.TRADE:
            stream_handlers['trades'] = self._trade_handler
          elif hname == sth.StreamHandlers.TRADE:
            stream_handlers['quotes'] = self._quote_handler
          elif hname == sth.StreamHandlers.TRADE:
            stream_handlers['bars'] = self._bar_handler

      self._api.register_stream_handlers(tuple(self._symbols), stream_handlers)

  def stop(self):
    with self._lock:
      started = self._started
      self._started -= 1

    if started == 1:
      self._api.register_stream_handlers([], dict())

