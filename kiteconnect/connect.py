import csv
import json
from io import StringIO
from datetime import datetime
import dateutil.parser

import kiteconnect.exeptions as ex
from kiteconnect.request import RequestSession
from kiteconnect.routes import Route

__all__ = ["KiteConnect", "Product", "Exchange", "OrderType", "Validity",
           "Variety", "TransactionType", "PositionType", "Margin"]


class Product:
    CNC = "CNC"
    MIS = "MIS"
    NRML = "NRML"
    CO = "CO"


class OrderType:
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    SLM = "SL-M"
    SL = "SL"


class Variety:
    REGULAR = "regular"
    CO = "co"
    AMO = "amo"
    ICEBERG = "iceberg"
    AUCTION = "auction"


class TransactionType:
    BUY = "BUY"
    SELL = "SELL"


class Validity:
    DAY = "DAY"
    IOC = "IOC"
    TTL = "TTL"


class PositionType:
    DAY = "day"
    OVERNIGHT = "overnight"


class Exchange:
    NSE = "NSE"
    BSE = "BSE"
    NFO = "NFO"
    CDS = "CDS"
    BFO = "BFO"
    MCX = "MCX"
    BCD = "BCD"


class Margin:
    EQUITY = "equity"
    COMMODITY = "commodity"


class Status:
    COMPLETE = "COMPLETE"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class GttType:
    OCO = "two-leg"
    SINGLE = "single"


class GttStatus:
    ACTIVE = "active"
    TRIGGERED = "triggered"
    DISABLED = "disabled"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    DELETED = "deleted"


class KiteConnect:
    _default_root_uri = "https://api.kite.trade"

    def __init__(self, apikey: str, access_token: str, root: str | None = None) -> None:
        self.on_token_expired: Callable | None = None

        self.reqSession = RequestSession(
            root=root if root else self._default_root_uri)
        self.reqSession.apikey = apikey
        self.reqSession.access_token = access_token
        self.reqSession.session_expiry_hook = self.on_token_expired

    def profile(self):
        """Get user profile details."""
        return self.reqSession.get(Route.USER_PROFILE)

    def margins(self, segment=None):
        """Get account balance and cash margin details for a particular segment.

        - `segment` is the trading segment (eg: equity or commodity)
        """
        if segment:
            return self.reqSession.get(
                Route.USER_MARGINS_SEGMENT, kwargs={"segment": segment})

        else:
            return self.reqSession.get(Route.USER_MARGINS)

    # order

    def place_order(self,
                    variety,
                    exchange,
                    tradingsymbol,
                    transaction_type,
                    quantity,
                    product,
                    order_type,
                    price=None,
                    validity=None,
                    validity_ttl=None,
                    disclosed_quantity=None,
                    trigger_price=None,
                    iceberg_legs=None,
                    iceberg_quantity=None,
                    auction_number=None,
                    tag=None):
        """Place an order."""

        data = locals()
        del (data["self"])

        for key, value in list(data.items()):
            if value is None:
                del (data[key])

        return self.reqSession.post(Route.ORDER_PLACE, kwargs={
                                    "variety": variety}, data=data)["order_id"]

    def modify_order(self,
                     variety,
                     order_id,
                     parent_order_id=None,
                     quantity=None,
                     price=None,
                     order_type=None,
                     trigger_price=None,
                     validity=None,
                     disclosed_quantity=None):
        """Modify an open order."""
        data = locals()
        del (data["self"])

        for key, value in list(data.items()):
            if value is None:
                del (data[key])

        return self.reqSession.put(Route.ORDER_MODIFY,
                                   kwargs={"variety": variety,
                                           "order_id": order_id},
                                   data=data)["order_id"]

    def cancel_order(self, variety, order_id, parent_order_id=None):
        """Cancel an order."""
        return self.reqSession.delete(Route.ORDER_CANCEL,
                                      kwargs={"variety": variety,
                                              "order_id": order_id},
                                      params={"parent_order_id": parent_order_id})["order_id"]

    def exit_order(self, variety, order_id, parent_order_id=None):
        """Exit a CO order."""
        return self.cancel_order(
            variety, order_id, parent_order_id=parent_order_id)

    def _format_response(self, data):
        """Parse and format responses."""
        _list = [data] if isinstance(data, dict) else data

        for item in _list:
            # Convert date time string to datetime object
            for field in ["order_timestamp", "exchange_timestamp", "created", "last_instalment", "fill_timestamp", "timestamp", "last_trade_time"]:
                if item.get(field) and len(item[field]) == 19:
                    item[field] = dateutil.parser.parse(item[field])

        return _list[0] if isinstance(data, dict) else _list

    # orderbook and tradebook
    def orders(self):
        """Get list of orders."""
        data = self.reqSession.get(Route.ORDERS)
        return self._format_response(data)

    def order_history(self, order_id):
        """
        Get history of individual order.

        - `order_id` is the ID of the order to retrieve order history.
        """
        data = self.reqSession.get(Route.ORDER_INFO, kwargs={
                                   "order_id": order_id})

        return self._format_response(data)

    def trades(self):
        """
        Retrieve the list of trades executed (all or ones under a particular order).

        An order can be executed in tranches based on market conditions.
        These trades are individually recorded under an order.
        """
        data = self.reqSession.get(Route.TRADES)
        return self._format_response(data)

    def order_trades(self, order_id):
        """
        Retrieve the list of trades executed for a particular order.

        - `order_id` is the ID of the order to retrieve trade history.
        """
        data = self.reqSession.get(Route.ORDER_TRADES, kwargs={
                                   "order_id": order_id})
        return self._format_response(data)

    def positions(self):
        """Retrieve the list of positions."""
        return self.reqSession.get(Route.PORTFOLIO_POSITIONS)

    def holdings(self):
        """Retrieve the list of equity holdings."""
        return self.reqSession.get(Route.PORTFOLIO_HOLDINGS)

    def get_auction_instruments(self):
        """ Retrieves list of available instruments for a auction session """
        return self.reqSession.get(Route.PORTFOLIO_HOLDINGS_AUCTION)

    def convert_position(self,
                         exchange,
                         tradingsymbol,
                         transaction_type,
                         position_type,
                         quantity,
                         old_product,
                         new_product):
        """Modify an open position's product type."""
        return self.reqSession.put(Route.PORTFOLIO_POSITIONS_CONVERT, data={
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "position_type": position_type,
            "quantity": quantity,
            "old_product": old_product,
            "new_product": new_product
        })

    # mf order

    def mf_orders(self, order_id=None):
        """Get all mutual fund orders or individual order info."""
        if order_id:
            return self.reqSession.get(Route.MF_ORDER_INFO, kwargs={
                                       "order_id": order_id})
            return self._format_response(self.reqSession.extract_json(response=resp))
        else:
            return self.reqSession.get(Route.MF_ORDERS)
            return self._format_response(self.reqSession.extract_json(response=resp))

    def place_mf_order(self,
                       tradingsymbol,
                       transaction_type,
                       quantity=None,
                       amount=None,
                       tag=None):
        """Place a mutual fund order."""
        return self.reqSession.post(Route.MF_ORDER_PLACE, data={
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "amount": amount,
            "tag": tag
        })

    def cancel_mf_order(self, order_id):
        """Cancel a mutual fund order."""
        return self.reqSession.delete(
            Route.MF_ORDER_CANCEL, kwargs={"order_id": order_id})

    def mf_sips(self, sip_id=None):
        """Get list of all mutual fund SIP's or individual SIP info."""
        if sip_id:
            return self.reqSession.get(
                Route.MF_SIP_INFO, kwargs={"sip_id": sip_id})
            return self._format_response(self.reqSession.extract_json(response=resp))
        else:
            return self.reqSession.get(Route.MF_SIPS)
            return self._format_response(self.reqSession.extract_json(response=resp))

    def place_mf_sip(self,
                     tradingsymbol,
                     amount,
                     instalments,
                     frequency,
                     initial_amount=None,
                     instalment_day=None,
                     tag=None):
        """Place a mutual fund SIP."""
        return self.reqSession.post(Route.MF_SIP_PLACE, data={
            "tradingsymbol": tradingsymbol,
            "amount": amount,
            "initial_amount": initial_amount,
            "instalments": instalments,
            "frequency": frequency,
            "instalment_day": instalment_day,
            "tag": tag
        })

    def modify_mf_sip(self,
                      sip_id,
                      amount=None,
                      status=None,
                      instalments=None,
                      frequency=None,
                      instalment_day=None):
        """Modify a mutual fund SIP."""
        return self.reqSession.put(Route.MF_SIP_MODIFY,
                                   kwargs={"sip_id": sip_id},
                                   data={
                                       "amount": amount,
                                       "status": status,
                                       "instalments": instalments,
                                       "frequency": frequency,
                                       "instalment_day": instalment_day
                                   })

    def cancel_mf_sip(self, sip_id):
        """Cancel a mutual fund SIP."""
        return self.reqSession.delete(
            Route.MF_SIP_CANCEL, kwargs={"sip_id": sip_id})

    def mf_holdings(self):
        """Get list of mutual fund holdings."""
        return self.reqSession.get(Route.MF_HOLDINGS)

    def mf_instruments(self) -> list[dict]:
        """Get list of mutual fund instruments."""
        data = self.reqSession.getcsv(Route.MF_INSTRUMENTS)
        return self._parse_mf_instruments(data)

    def instruments(self, exchange=None) -> list[dict]:
        """
        Retrieve the list of market instruments available to trade.

        Note that the results could be large, several hundred KBs in size,
        with tens of thousands of entries in the list.

        - `exchange` is specific exchange to fetch (Optional)
        """
        if exchange:
            data = self.reqSession.getcsv(Route.MARKET_INSTRUMENTS, kwargs={
                "exchange": exchange})
            return self._parse_instruments(data)
        else:
            data = self.reqSession.get(Route.MARKET_INSTRUMENTS_ALL)
            return self._parse_instruments(data)

    # market data

    def quote(self, *instruments):
        """
        Retrieve quote for list of instruments.

        - `instruments` is a list of instruments, Instrument are in the format of `exchange:tradingsymbol`. For example NSE:INFY
        """

        # If first element is a list then accept it as instruments list for legacy reason
        ins = instruments[0] if instruments and isinstance(
            instruments[0], list) else list(instruments)

        data = self.reqSession.get(Route.MARKET_QUOTE, params={"i": ins})
        return {key: self._format_response(value) for key, value in data.items()}

    def ohlc(self, *instruments):
        """
        Retrieve OHLC and market depth for list of instruments.

        - `instruments` is a list of instruments, Instrument are in the format of `exchange:tradingsymbol`. For example NSE:INFY
        """
        # If first element is a list then accept it as instruments list for legacy reason
        ins = instruments[0] if instruments and isinstance(
            instruments[0], list) else list(instruments)

        return self.reqSession.get(Route.MARKET_QUOTE_OHLC, params={"i": ins})

    def ltp(self, *instruments):
        """
        Retrieve last price for list of instruments.

        - `instruments` is a list of instruments, Instrument are in the format of `exchange:tradingsymbol`. For example NSE:INFY
        """
        # If first element is a list then accept it as instruments list for legacy reason
        ins = instruments[0] if instruments and isinstance(
            instruments[0], list) else list(instruments)

        return self.reqSession.get(Route.MARKET_QUOTE_LTP, params={"i": ins})

    def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
        """
        Retrieve historical data (candles) for an instrument.

        Although the actual response JSON from the API does not have field
        names such has 'open', 'high' etc., this function call structures
        the data into an array of objects with field names. For example:

        - `instrument_token` is the instrument identifier (retrieved from the instruments()) call.
        - `from_date` is the From date (datetime object or string in format of yyyy-mm-dd HH:MM:SS.
        - `to_date` is the To date (datetime object or string in format of yyyy-mm-dd HH:MM:SS).
        - `interval` is the candle interval (minute, day, 5 minute etc.).
        - `continuous` is a boolean flag to get continuous data for futures and options instruments.
        - `oi` is a boolean flag to get open interest.
        """
        date_string_format = "%Y-%m-%d %H:%M:%S"
        from_date_string = from_date.strftime(date_string_format) if isinstance(
            from_date, datetime) else from_date
        to_date_string = to_date.strftime(date_string_format) if isinstance(
            to_date, datetime) else to_date

        data = self.reqSession.get(Route.MARKET_HISTORICAL,
                                   kwargs={"instrument_token": instrument_token,
                                           "interval": interval},
                                   params={
                                       "from": from_date_string,
                                       "to": to_date_string,
                                       "interval": interval,
                                       "continuous": 1 if continuous else 0,
                                       "oi": 1 if oi else 0
                                   })

        return self._format_historical(data)

    def _format_historical(self, data):
        records = []
        for d in data["candles"]:
            record = {
                "date": dateutil.parser.parse(d[0]),
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
            }
            if len(d) == 7:
                record["oi"] = d[6]
            records.append(record)

        return records

    def trigger_range(self, transaction_type, *instruments):
        """Retrieve the buy/sell trigger range for Cover Orders."""
        # If first element is a list then accept it as instruments list for legacy reason
        ins = instruments[0] if instruments and isinstance(
            instruments[0], list) else list(instruments)

        return self.reqSession.get(Route.MARKET_TRIGGER_RANGE,
                                   kwargs={
                                       "transaction_type": transaction_type.lower()},
                                   params={"i": ins})

    def get_gtts(self):
        """Fetch list of gtt existing in an account"""
        return self.reqSession.get("gtt")

    def get_gtt(self, trigger_id):
        """Fetch details of a GTT"""
        return self.reqSession.get(
            Route.GTT_INFO, kwargs={"trigger_id": trigger_id})

    def _get_gtt_payload(self, trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders):
        """Get GTT payload"""
        if not isinstance(trigger_values, list):
            raise ex.InputException("invalid type for `trigger_values`")
        if trigger_type == GttType.SINGLE and len(trigger_values) != 1:
            raise ex.InputException(
                "invalid `trigger_values` for single leg order type")
        elif trigger_type == GttType.OCO and len(trigger_values) != 2:
            raise ex.InputException(
                "invalid `trigger_values` for OCO order type")

        condition = {
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "trigger_values": trigger_values,
            "last_price": last_price,
        }

        gtt_orders = []
        for o in orders:
            # Assert required keys inside gtt order.
            for req in ["transaction_type", "quantity", "order_type", "product", "price"]:
                if req not in o:
                    raise ex.InputException(
                        "`{req}` missing inside orders".format(req=req))
            gtt_orders.append({
                "exchange": exchange,
                "tradingsymbol": tradingsymbol,
                "transaction_type": o["transaction_type"],
                "quantity": int(o["quantity"]),
                "order_type": o["order_type"],
                "product": o["product"],
                "price": float(o["price"]),
            })

        return condition, gtt_orders

    def place_gtt(self, trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders):
        """
        Place GTT order

        - `trigger_type` The type of GTT order(single/two-leg).
        - `tradingsymbol` Trading symbol of the instrument.
        - `exchange` Name of the exchange.
        - `trigger_values` Trigger values (json array).
        - `last_price` Last price of the instrument at the time of order placement.
        - `orders` JSON order array containing following fields
            - `transaction_type` BUY or SELL
            - `quantity` Quantity to transact
            - `price` The min or max price to execute the order at (for LIMIT orders)
        """
        # Validations.
        assert trigger_type in [GttType.SINGLE, GttType.OCO]
        condition, gtt_orders = self._get_gtt_payload(
            trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders)

        return self.reqSession.post(Route.GTT_PLACE, data={
            "condition": json.dumps(condition),
            "orders": json.dumps(gtt_orders),
            "type": trigger_type})

    def modify_gtt(self, trigger_id, trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders):
        """
        Modify GTT order

        - `trigger_type` The type of GTT order(single/two-leg).
        - `tradingsymbol` Trading symbol of the instrument.
        - `exchange` Name of the exchange.
        - `trigger_values` Trigger values (json array).
        - `last_price` Last price of the instrument at the time of order placement.
        - `orders` JSON order array containing following fields
            - `transaction_type` BUY or SELL
            - `quantity` Quantity to transact
            - `price` The min or max price to execute the order at (for LIMIT orders)
        """
        condition, gtt_orders = self._get_gtt_payload(
            trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders)

        return self.reqSession.put(Route.GTT_MODIFY,
                                   kwargs={"trigger_id": trigger_id},
                                   data={
                                       "condition": json.dumps(condition),
                                       "orders": json.dumps(gtt_orders),
                                       "type": trigger_type})

    def delete_gtt(self, trigger_id):
        """Delete a GTT order."""
        return self.reqSession.delete(
            Route.GTT_DELETE, kwargs={"trigger_id": trigger_id})

    def order_margins(self, data: dict):
        """
        Calculate margins for requested order list considering the existing positions and open orders

        - `data` is list of orders to retrive margins detail
        """
        return self.reqSession.post(Route.ORDER_MARGINS, json=data)

    def basket_order_margins(self, data: dict, consider_positions=True, mode=None):
        """
        Calculate total margins required for basket of orders including margin benefits

        - `data` is list of orders to fetch basket margin
        - `consider_positions` is a boolean to consider users positions
        - `mode` is margin response mode type. compact - Compact mode will only give the total margins
        """
        return self.reqSession.post(Route.ORDER_MARGINS_BASKET,
                                    json=data,
                                    params={'consider_positions': consider_positions, 'mode': mode})

    def _parse_instruments(self, data: bytes) -> list[dict]:
        records = []
        reader = csv.DictReader(StringIO(data))

        for row in reader:
            row["instrument_token"] = int(row["instrument_token"])
            row["last_price"] = float(row["last_price"])
            row["strike"] = float(row["strike"])
            row["tick_size"] = float(row["tick_size"])
            row["lot_size"] = int(row["lot_size"])

            # Parse date
            if len(row["expiry"]) == 10:
                row["expiry"] = dateutil.parser.parse(row["expiry"]).date()

            records.append(row)

        return records

    def _parse_mf_instruments(self, data) -> list[dict]:
        records = []
        reader = csv.DictReader(StringIO(data))

        for row in reader:
            row["minimum_purchase_amount"] = float(
                row["minimum_purchase_amount"])
            row["purchase_amount_multiplier"] = float(
                row["purchase_amount_multiplier"])
            row["minimum_additional_purchase_amount"] = float(
                row["minimum_additional_purchase_amount"])
            row["minimum_redemption_quantity"] = float(
                row["minimum_redemption_quantity"])
            row["redemption_quantity_multiplier"] = float(
                row["redemption_quantity_multiplier"])
            row["purchase_allowed"] = bool(int(row["purchase_allowed"]))
            row["redemption_allowed"] = bool(int(row["redemption_allowed"]))
            row["last_price"] = float(row["last_price"])

            # Parse date
            if len(row["last_price_date"]) == 10:
                row["last_price_date"] = dateutil.parser.parse(
                    row["last_price_date"]).date()

            records.append(row)

        return records
