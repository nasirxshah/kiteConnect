import logging
from kiteconnect import KiteConnect, KiteSession
from kiteconnect.connect import *
from kiteconnect.exeptions import *
logging.basicConfig(level=logging.DEBUG)

# Redirect the user to the login url obtained
# from kite.login_url(), and receive the request_token
# from the registered redirect url after the login flow.
# Once you have the request_token, obtain the access_token
# as follows.

ksession = KiteSession(apikey="api_key")
ksession.generate_session(
    request_token="request_token", api_secret="your_secret")

kite = KiteConnect(apikey="ksession.apikey",
                   access_token="ksession.access_token")
# Place an order
try:
    order_id = kite.place_order(tradingsymbol="INFY",
                                exchange=Exchange.NSE,
                                transaction_type=TransactionType.BUY,
                                quantity=1,
                                variety=Variety.AMO,
                                order_type=OrderType.MARKET,
                                product=Product.CNC,
                                validity=Validity.DAY)

    logging.info("Order placed. ID is: {}".format(order_id))

except KiteException as e:
    logging.info("Order placement failed: {}".format(e.message))

# Fetch all orders
kite.orders()

# Get instruments
kite.instruments()

# Place an mutual fund order
kite.place_mf_order(
    tradingsymbol="INF090I01239",
    transaction_type=TransactionType.BUY,
    amount=5000,
    tag="mytag"
)

# Cancel a mutual fund order
kite.cancel_mf_order(order_id="order_id")

# Get mutual fund instruments
kite.mf_instruments()
