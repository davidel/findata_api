import py_misc_utils.state as pyst

from . import order_tracker


class API(pyst.StateBase):

  def __init__(self, name=None, supports_streaming=False, supports_trading=False):
    super().__init__()
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

    self._store_state(__class__, refresh_time=refresh_time)

    self.tracker = order_tracker.OrderTracker(self,
                                              scheduler=scheduler,
                                              refresh_time=refresh_time)
    self.scheduler = self.tracker.scheduler

  def _get_state(self, state):
    cstate = API._get_state(self, state)
    cstate.pop('tracker')
    cstate.pop('scheduler')

    return cstate

  def _set_state(self, state):
    scheduler = state.pop('scheduler')
    refresh_time = self._load_state(__class__, state, 'refresh_time')

    API._set_state(self, state)
    self.tracker = order_tracker.OrderTracker(self,
                                              scheduler=scheduler,
                                              refresh_time=refresh_time)
    self.scheduler = self.tracker.scheduler

