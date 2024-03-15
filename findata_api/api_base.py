from . import order_tracker


class API:

  def __init__(self, name=None, supports_streaming=False, supports_trading=False):
    self.name = name
    self.supports_streaming = supports_streaming
    self.supports_trading = supports_trading

  def close(self):
    pass

  def range_supported(self, start_date, end_date, data_step):
    return True


class TradeAPI(API):

  def __init__(self, scheduler=None, refresh_time=None, **kwargs):
    super().__init__(supports_trading=True, **kwargs)
    self.tracker = order_tracker.OrderTracker(self,
                                              scheduler=scheduler,
                                              refresh_time=refresh_time)

