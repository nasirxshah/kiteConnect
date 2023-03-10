

class Route:
    API_TOKEN = '/session/token'
    API_TOKEN_INVALIDATE = '/session/token'
    API_TOKEN_RENEW = '/session/refresh_token'
    USER_PROFILE = '/user/profile'
    USER_MARGINS = '/user/margins'
    USER_MARGINS_SEGMENT = '/user/margins/{segment}'
    ORDERS = '/orders'
    TRADES = '/trades'
    ORDER_INFO = '/orders/{order_id}'
    ORDER_PLACE = '/orders/{variety}'
    ORDER_MODIFY = '/orders/{variety}/{order_id}'
    ORDER_CANCEL = '/orders/{variety}/{order_id}'
    ORDER_TRADES = '/orders/{order_id}/trades'
    PORTFOLIO_POSITIONS = '/portfolio/positions'
    PORTFOLIO_HOLDINGS = '/portfolio/holdings'
    PORTFOLIO_HOLDINGS_AUCTION = '/portfolio/holdings/auctions'
    PORTFOLIO_POSITIONS_CONVERT = '/portfolio/positions'
    MF_ORDERS = '/mf/orders'
    MF_ORDER_INFO = '/mf/orders/{order_id}'
    MF_ORDER_PLACE = '/mf/orders'
    MF_ORDER_CANCEL = '/mf/orders/{order_id}'
    MF_SIPS = '/mf/sips'
    MF_SIP_INFO = '/mf/sips/{sip_id}'
    MF_SIP_PLACE = '/mf/sips'
    MF_SIP_MODIFY = '/mf/sips/{sip_id}'
    MF_SIP_CANCEL = '/mf/sips/{sip_id}'
    MF_HOLDINGS = '/mf/holdings'
    MF_INSTRUMENTS = '/mf/instruments'
    MARKET_INSTRUMENTS_ALL = '/instruments'
    MARKET_INSTRUMENTS = '/instruments/{exchange}'
    MARKET_MARGINS = '/margins/{segment}'
    MARKET_HISTORICAL = '/instruments/historical/{instrument_token}/{interval}'
    MARKET_TRIGGER_RANGE = '/instruments/trigger_range/{transaction_type}'
    MARKET_QUOTE = '/quote'
    MARKET_QUOTE_OHLC = '/quote/ohlc'
    MARKET_QUOTE_LTP = '/quote/ltp'
    GTT = '/gtt/triggers'
    GTT_PLACE = '/gtt/triggers'
    GTT_INFO = '/gtt/triggers/{trigger_id}'
    GTT_MODIFY = '/gtt/triggers/{trigger_id}'
    GTT_DELETE = '/gtt/triggers/{trigger_id}'
    ORDER_MARGINS = '/margins/orders'
    ORDER_MARGINS_BASKET = '/margins/basket'
