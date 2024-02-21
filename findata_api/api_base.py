
class API:

  def range_supported(self, start_date, end_date, data_step):
    return True

  @property
  def supports_streaming(self):
    return False

  @property
  def supports_trading(self):
    return False

