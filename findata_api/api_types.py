import collections

Order = collections.namedtuple(
    'Order',
    'id, symbol, quantity, side, type, limit, stop, status, created, filled, filled_quantity, filled_avg_price')

Position = collections.namedtuple(
    'Position',
    'symbol, quantity, value')

Account = collections.namedtuple(
    'Account',
    'id, buying_power')

StreamTrade = collections.namedtuple(
    'StreamTrade',
    'timestamp, symbol, quantity, price')

StreamQuote = collections.namedtuple(
    'StreamQuote',
    'timestamp, symbol, bid_size, bid_price, ask_size, ask_price')

