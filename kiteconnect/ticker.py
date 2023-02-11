import json
import logging
import struct
import sys
import threading
import time
from datetime import datetime
from typing import Callable

from autobahn.twisted.websocket import (WebSocketClientFactory,
                                        WebSocketClientProtocol, connectWS)
from twisted.internet import reactor, ssl
from twisted.internet.epollreactor import EPollReactor
from twisted.internet.pollreactor import PollReactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.selectreactor import SelectReactor
from twisted.python import log as twisted_log

from .__version__ import __title__, __version__

logger = logging.getLogger(__name__)

reactor: EPollReactor | PollReactor | SelectReactor


class KiteTickerClientProtocol(WebSocketClientProtocol):
    """Kite ticker autobahn WebSocket protocol."""

    PING_INTERVAL = 2.5
    KEEPALIVE_INTERVAL = 5

    _ping_message = ""
    _next_ping = None
    _next_pong_check = None
    _last_pong_time = None
    _last_ping_time = None

    def __init__(self, *args, **kwargs):
        """Initialize protocol with all options passed from factory."""
        super(KiteTickerClientProtocol, self).__init__(*args, **kwargs)

        self.on_connect = None

    # Overide method

    def onConnect(self, response):  # noqa
        """Called when WebSocket server connection was established"""
        self.factory: KiteTickerClientFactory
        self.factory.ws = self

        if self.on_connect:
            self.on_connect(self, response)

        # Reset reconnect on successful reconnect
        self.factory.resetDelay()

    # Overide method
    def onOpen(self):  # noqa
        """Called when the initial WebSocket opening handshake was completed."""
        # send ping
        self._loop_ping()
        # init last pong check after X seconds
        self._loop_pong_check()

        if self.factory.on_open:
            self.factory.on_open(self)

    # Overide method
    def onMessage(self, payload, is_binary):  # noqa
        """Called when text or binary message is received."""
        if self.factory.on_message:
            self.factory.on_message(self, payload, is_binary)

    # Overide method
    def onClose(self, was_clean, code, reason):  # noqa
        """Called when connection is closed."""
        if not was_clean:
            if self.factory.on_error:
                self.factory.on_error(self, code, reason)

        if self.factory.on_close:
            self.factory.on_close(self, code, reason)

        # Cancel next ping and timer
        self._last_ping_time = None
        self._last_pong_time = None

        if self._next_ping:
            self._next_ping.cancel()

        if self._next_pong_check:
            self._next_pong_check.cancel()

    def onPong(self, response):  # noqa
        """Called when pong message is received."""
        if self._last_pong_time and self.factory.debug:
            logger.debug("last pong was {} seconds back.".format(
                time.time() - self._last_pong_time))

        self._last_pong_time = time.time()

        if self.factory.debug:
            logger.debug("pong => {}".format(response))

    """
    Custom helper and exposed methods.
    """

    def _loop_ping(self):  # noqa
        """Start a ping loop where it sends ping message every X seconds."""
        if self.factory.debug:
            logger.debug("ping => {}".format(self._ping_message))
            if self._last_ping_time:
                logger.debug("last ping was {} seconds back.".format(
                    time.time() - self._last_ping_time))

        # Set current time as last ping time
        self._last_ping_time = time.time()
        # Send a ping message to server
        self.sendPing(self._ping_message)

        # Call self after X seconds
        self._next_ping = reactor.callLater(
            self.PING_INTERVAL, self._loop_ping)

    def _loop_pong_check(self):
        """
        Timer sortof to check if connection is still there.

        Checks last pong message time and disconnects the existing connection to make sure it doesn't become a ghost connection.
        """
        if self._last_pong_time:
            # No pong message since long time, so init reconnect
            last_pong_diff = time.time() - self._last_pong_time
            if last_pong_diff > (2 * self.PING_INTERVAL):
                if self.factory.debug:
                    logger.debug("Last pong was {} seconds ago. So dropping connection to reconnect.".format(
                        last_pong_diff))
                # drop existing connection to avoid ghost connection
                self.dropConnection(abort=True)

        # Call self after X seconds
        self._next_pong_check = reactor.callLater(
            self.PING_INTERVAL, self._loop_pong_check)


class KiteTickerClientFactory(WebSocketClientFactory, ReconnectingClientFactory):
    """Autobahn WebSocket client factory to implement reconnection and custom callbacks."""

    protocol = KiteTickerClientProtocol
    maxDelay = 5
    maxRetries = 10

    _last_connection_time = None

    def __init__(self, *args, **kwargs):
        """Initialize with default callback method values."""
        self.debug = False
        self.ws: KiteTickerClientProtocol | None = None
        self.on_open: Callable | None = None
        self.on_error: Callable | None = None
        self.on_close: Callable | None = None
        self.on_message: Callable | None = None
        self.on_connect: Callable | None = None
        self.on_reconnect: Callable | None = None
        self.on_noreconnect: Callable | None = None

        super(KiteTickerClientFactory, self).__init__(*args, **kwargs)

    def startedConnecting(self, connector):  # noqa
        """On connecting start or reconnection."""
        if not self._last_connection_time and self.debug:
            logger.debug("Start WebSocket connection.")

        self._last_connection_time = time.time()

    def clientConnectionFailed(self, connector, reason):  # noqa
        """On connection failure (When connect request fails)"""
        if self.retries > 0:
            logger.error("Retrying connection. Retry attempt count: {}. Next retry in around: {} seconds".format(
                self.retries, int(round(self.delay))))

            # on reconnect callback
            if self.on_reconnect:
                self.on_reconnect(self.retries)

        # Retry the connection
        self.retry(connector)
        self.send_noreconnect()

    def clientConnectionLost(self, connector, reason):  # noqa
        """On connection lost (When ongoing connection got disconnected)."""
        if self.retries > 0:
            # on reconnect callback
            if self.on_reconnect:
                self.on_reconnect(self.retries)

        # Retry the connection
        self.retry(connector)
        self.send_noreconnect()

    def send_noreconnect(self):
        """Callback `no_reconnect` if max retries are exhausted."""
        if self.maxRetries is not None and (self.retries > self.maxRetries):
            if self.debug:
                logger.debug("Maximum retries ({}) exhausted.".format(
                    self.maxRetries))
                # Stop the loop for exceeding max retry attempts
                self.stop()

            if self.on_noreconnect:
                self.on_noreconnect()


class Mode:
    FULL = "full"
    QUOTE = "quote"
    LTP = "ltp"


class KiteTicker(object):
    class Exchange:
        NSE = 1
        NFO = 2
        CDS = 3
        BSE = 4
        BFO = 5
        BCD = 6
        MCX = 7
        MCXSX = 8
        INDICES = 9
        # bsecds is replaced with it's official segment name bcd
        # so,bsecds key will be depreciated in next version
        BSECDS = 6

    class Message:
        CODE = 11
        SUBSCRIBE = "subscribe"
        UNSUBSCRIBE = "unsubscribe"
        SETMODE = "mode"

    # Default connection timeout
    CONNECT_TIMEOUT = 30
    # Default Reconnect max delay.
    RECONNECT_MAX_DELAY = 60
    # Default reconnect attempts
    RECONNECT_MAX_TRIES = 50
    # Default root API endpoint. It's possible to
    # override this by passing the `root` parameter during initialisation.
    ROOT_URI = "wss://ws.kite.trade"

    # Flag to set if its first connect
    _is_first_connect = True
    # Minimum delay which should be set between retries. User can't set less than this
    _minimum_reconnect_max_delay = 5
    # Maximum number or retries user can set
    _maximum_reconnect_max_tries = 300

    def __init__(self, api_key, access_token, debug=False, root=None,
                 reconnect=True, reconnect_max_tries=RECONNECT_MAX_TRIES, reconnect_max_delay=RECONNECT_MAX_DELAY,
                 connect_timeout=CONNECT_TIMEOUT):
        """
        Initialise websocket client instance.

        - `api_key` is the API key issued to you
        - `access_token` is the token obtained after the login flow in
            exchange for the `request_token`. Pre-login, this will default to None,
            but once you have obtained it, you should
            persist it in a database or session to pass
            to the Kite Connect class initialisation for subsequent requests.
        - `root` is the websocket API end point root. Unless you explicitly
            want to send API requests to a non-default endpoint, this
            can be ignored.
        - `reconnect` is a boolean to enable WebSocket autreconnect in case of network failure/disconnection.
        - `reconnect_max_delay` in seconds is the maximum delay after which subsequent reconnection interval will become constant. Defaults to 60s and minimum acceptable value is 5s.
        - `reconnect_max_tries` is maximum number reconnection attempts. Defaults to 50 attempts and maximum up to 300 attempts.
        - `connect_timeout` in seconds is the maximum interval after which connection is considered as timeout. Defaults to 30s.
        """
        self.root = root or self.ROOT_URI

        # Set max reconnect tries
        if reconnect_max_tries > self._maximum_reconnect_max_tries:
            logger.warning("`reconnect_max_tries` can not be more than {val}. Setting to highest possible value - {val}.".format(
                val=self._maximum_reconnect_max_tries))
            self.reconnect_max_tries = self._maximum_reconnect_max_tries
        else:
            self.reconnect_max_tries = reconnect_max_tries

        # Set max reconnect delay
        if reconnect_max_delay < self._minimum_reconnect_max_delay:
            logger.warning("`reconnect_max_delay` can not be less than {val}. Setting to lowest possible value - {val}.".format(
                val=self._minimum_reconnect_max_delay))
            self.reconnect_max_delay = self._minimum_reconnect_max_delay
        else:
            self.reconnect_max_delay = reconnect_max_delay

        self.connect_timeout = connect_timeout

        self.socket_url = f"{self.root}?api_key={api_key}&access_token={access_token}"

        # Debug enables logs
        self.debug = debug

        # Initialize default value for websocket object
        self.ws: KiteTickerClientProtocol | None = None

        # Placeholders for callbacks.
        self.on_ticks: Callable | None = None
        self.on_open: Callable | None = None
        self.on_close: Callable | None = None
        self.on_error: Callable | None = None
        self.on_connect: Callable | None = None
        self.on_message: Callable | None = None
        self.on_reconnect: Callable | None = None
        self.on_noreconnect: Callable | None = None

        # Text message updates
        self.on_order_update: Callable | None = None

        # List of current subscribed tokens
        self.subscribed_tokens = {}

    def _create_connection(self, url, **kwargs):
        """Create a WebSocket client connection."""
        self.factory = KiteTickerClientFactory(url, **kwargs)

        # Alias for current websocket connection
        self.ws = self.factory.ws

        self.factory.debug = self.debug

        # Register private callbacks
        self.factory.on_open = self._on_open
        self.factory.on_error = self._on_error
        self.factory.on_close = self._on_close
        self.factory.on_message = self._on_message
        self.factory.on_connect = self._on_connect
        self.factory.on_reconnect = self._on_reconnect
        self.factory.on_noreconnect = self._on_noreconnect

        self.factory.maxDelay = self.reconnect_max_delay
        self.factory.maxRetries = self.reconnect_max_tries

    def _user_agent(self):
        return (__title__ + "-python/").capitalize() + __version__

    def connect(self, threaded=False, disable_ssl_verification=False, proxy=None):
        """
        Establish a websocket connection.

        - `threaded` is a boolean indicating if the websocket client has to be run in threaded mode or not
        - `disable_ssl_verification` disables building ssl context
        - `proxy` is a dictionary with keys `host` and `port` which denotes the proxy settings
        """
        # Custom headers
        headers = {
            "X-Kite-Version": "3",  # For version 3
        }

        # Init WebSocket client factory
        self._create_connection(self.socket_url,
                                useragent=self._user_agent(),
                                proxy=proxy, headers=headers)

        # Set SSL context
        context_factory = None
        if self.factory.isSecure and not disable_ssl_verification:
            context_factory = ssl.ClientContextFactory()

        # Establish WebSocket connection to a server
        connectWS(self.factory, contextFactory=context_factory,
                  timeout=self.connect_timeout)

        if self.debug:
            twisted_log.startLogging(sys.stdout)

        # Run in seperate thread of blocking
        opts = {}
        # Run when reactor is not running
        if not reactor.running:
            if threaded:
                # Signals are not allowed in non main thread by twisted so suppress it.
                opts["installSignalHandlers"] = False
                self.websocket_thread = threading.Thread(
                    target=reactor.run, kwargs=opts)
                self.websocket_thread.daemon = True
                self.websocket_thread.start()
            else:
                reactor.run(**opts)

    def is_connected(self):
        """Check if WebSocket connection is established."""
        if self.ws and self.ws.state == self.ws.STATE_OPEN:
            return True
        else:
            return False

    def _close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        if self.ws:
            self.ws.sendClose(code, reason)

    def close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        self.stop_retry()
        self._close(code, reason)

    def stop(self):
        """Stop the event loop. Should be used if main thread has to be closed in `on_close` method.
        Reconnection mechanism cannot happen past this method
        """
        reactor.stop()

    def stop_retry(self):
        """Stop auto retry when it is in progress."""
        if self.factory:
            self.factory.stopTrying()

    def subscribe(self, instrument_tokens):
        """
        Subscribe to a list of instrument_tokens.

        - `instrument_tokens` is list of instrument instrument_tokens to subscribe
        """
        try:
            assert self.ws is not None
            self.ws.sendMessage(json.dumps({
                "a": self.Message.SUBSCRIBE,
                "v": instrument_tokens}).encode()
            )

            for token in instrument_tokens:
                self.subscribed_tokens[token] = Mode.QUOTE

            return True
        except Exception as e:
            self._close(reason="Error while subscribe: {}".format(str(e)))
            raise

    def unsubscribe(self, instrument_tokens):
        """
        Unsubscribe the given list of instrument_tokens.

        - `instrument_tokens` is list of instrument_tokens to unsubscribe.
        """
        try:
            assert self.ws is not None
            self.ws.sendMessage(json.dumps({
                "a": self.Message.UNSUBSCRIBE,
                "v": instrument_tokens}).encode()
            )

            for token in instrument_tokens:
                try:
                    del (self.subscribed_tokens[token])
                except KeyError:
                    pass

            return True
        except Exception as e:
            self._close(reason="Error while unsubscribe: {}".format(str(e)))
            raise

    def set_mode(self, mode, instrument_tokens):
        """
        Set streaming mode for the given list of tokens.

        - `mode` is the mode to set. It can be one of the following class constants:
            MODE_LTP, MODE_QUOTE, or MODE_FULL.
        - `instrument_tokens` is list of instrument tokens on which the mode should be applied
        """
        try:
            assert self.ws is not None
            self.ws.sendMessage(json.dumps({
                "a": self.Message.SETMODE,
                "v": [mode, instrument_tokens]}).encode()
            )

            # Update modes
            for token in instrument_tokens:
                self.subscribed_tokens[token] = mode

            return True
        except Exception as e:
            self._close(reason="Error while setting mode: {}".format(str(e)))
            raise

    def resubscribe(self):
        """Resubscribe to all current subscribed tokens."""
        modes = {}

        for token in self.subscribed_tokens:
            m = self.subscribed_tokens[token]

            if not modes.get(m):
                modes[m] = []

            modes[m].append(token)

        for mode in modes:
            if self.debug:
                logger.debug(
                    "Resubscribe and set mode: {} - {}".format(mode, modes[mode]))

            self.subscribe(modes[mode])
            self.set_mode(mode, modes[mode])

    def _on_connect(self, ws, response):
        self.ws: KiteTickerClientProtocol | None = ws
        if self.on_connect:
            self.on_connect(self, response)

    def _on_close(self, ws, code, reason):
        """Call `on_close` callback when connection is closed."""
        logger.error("Connection closed: {} - {}".format(code, str(reason)))

        if self.on_close:
            self.on_close(self, code, reason)

    def _on_error(self, ws, code, reason):
        """Call `on_error` callback when connection throws an error."""
        logger.error("Connection error: {} - {}".format(code, str(reason)))

        if self.on_error:
            self.on_error(self, code, reason)

    def _on_message(self, ws, payload, is_binary):
        """Call `on_message` callback when text message is received."""
        if self.on_message:
            self.on_message(self, payload, is_binary)

        # If the message is binary, parse it and send it to the callback.
        if self.on_ticks and is_binary and len(payload) > 4:
            self.on_ticks(self, self._parse_binary(payload))

        # Parse text messages
        if not is_binary:
            self._parse_text_message(payload)

    def _on_open(self, ws):
        # Resubscribe if its reconnect
        if not self._is_first_connect:
            self.resubscribe()

        # Set first connect to false once its connected first time
        self._is_first_connect = False

        if self.on_open:
            return self.on_open(self)

    def _on_reconnect(self, attempts_count):
        if self.on_reconnect:
            return self.on_reconnect(self, attempts_count)

    def _on_noreconnect(self):
        if self.on_noreconnect:
            return self.on_noreconnect(self)

    def _parse_text_message(self, payload):
        """Parse text message."""
        try:
            data = json.loads(payload)
        except ValueError:
            return

        # Order update callback
        if self.on_order_update and data.get("type") == "order" and data.get("data"):
            self.on_order_update(self, data["data"])

        # Custom error with websocket error code 0
        if data.get("type") == "error":
            self._on_error(self, 0, data.get("data"))

    def _parse_binary(self, bin):
        """Parse binary data to a (list of) ticks structure."""
        packets = self._split_packets(
            bin)  # split data to individual ticks packet
        data = []

        for packet in packets:
            instrument_token = self._unpack_int(packet, 0, 4)
            # Retrive segment constant from instrument_token
            segment = instrument_token & 0xff

            # Add price divisor based on segment
            if segment == self.Exchange.CDS:
                divisor = 10000000.0
            elif segment == self.Exchange.BCD:
                divisor = 10000.0
            else:
                divisor = 100.0

            # All indices are not tradable
            tradable = False if segment == self.Exchange.INDICES else True

            # LTP packets
            if len(packet) == 8:
                data.append({
                    "tradable": tradable,
                    "mode": Mode.LTP,
                    "instrument_token": instrument_token,
                    "last_price": self._unpack_int(packet, 4, 8) / divisor
                })
            # Indices quote and full mode
            elif len(packet) == 28 or len(packet) == 32:
                mode = Mode.QUOTE if len(packet) == 28 else Mode.FULL

                d = {
                    "tradable": tradable,
                    "mode": mode,
                    "instrument_token": instrument_token,
                    "last_price": self._unpack_int(packet, 4, 8) / divisor,
                    "ohlc": {
                        "high": self._unpack_int(packet, 8, 12) / divisor,
                        "low": self._unpack_int(packet, 12, 16) / divisor,
                        "open": self._unpack_int(packet, 16, 20) / divisor,
                        "close": self._unpack_int(packet, 20, 24) / divisor
                    }
                }

                # Compute the change price using close price and last price
                d["change"] = 0
                if (d["ohlc"]["close"] != 0):
                    d["change"] = (d["last_price"] - d["ohlc"]
                                   ["close"]) * 100 / d["ohlc"]["close"]

                # Full mode with timestamp
                if len(packet) == 32:
                    try:
                        timestamp = datetime.fromtimestamp(
                            self._unpack_int(packet, 28, 32))
                    except Exception:
                        timestamp = None

                    d["exchange_timestamp"] = timestamp

                data.append(d)
            # Quote and full mode
            elif len(packet) == 44 or len(packet) == 184:
                mode = Mode.QUOTE if len(packet) == 44 else Mode.FULL

                d = {
                    "tradable": tradable,
                    "mode": mode,
                    "instrument_token": instrument_token,
                    "last_price": self._unpack_int(packet, 4, 8) / divisor,
                    "last_traded_quantity": self._unpack_int(packet, 8, 12),
                    "average_traded_price": self._unpack_int(packet, 12, 16) / divisor,
                    "volume_traded": self._unpack_int(packet, 16, 20),
                    "total_buy_quantity": self._unpack_int(packet, 20, 24),
                    "total_sell_quantity": self._unpack_int(packet, 24, 28),
                    "ohlc": {
                        "open": self._unpack_int(packet, 28, 32) / divisor,
                        "high": self._unpack_int(packet, 32, 36) / divisor,
                        "low": self._unpack_int(packet, 36, 40) / divisor,
                        "close": self._unpack_int(packet, 40, 44) / divisor
                    }
                }

                # Compute the change price using close price and last price
                d["change"] = 0
                if (d["ohlc"]["close"] != 0):
                    d["change"] = (d["last_price"] - d["ohlc"]
                                   ["close"]) * 100 / d["ohlc"]["close"]

                # Parse full mode
                if len(packet) == 184:
                    try:
                        last_trade_time = datetime.fromtimestamp(
                            self._unpack_int(packet, 44, 48))
                    except Exception:
                        last_trade_time = None

                    try:
                        timestamp = datetime.fromtimestamp(
                            self._unpack_int(packet, 60, 64))
                    except Exception:
                        timestamp = None

                    d["last_trade_time"] = last_trade_time
                    d["oi"] = self._unpack_int(packet, 48, 52)
                    d["oi_day_high"] = self._unpack_int(packet, 52, 56)
                    d["oi_day_low"] = self._unpack_int(packet, 56, 60)
                    d["exchange_timestamp"] = timestamp

                    # Market depth entries.
                    depth = {
                        "buy": [],
                        "sell": []
                    }

                    # Compile the market depth lists.
                    for i, p in enumerate(range(64, len(packet), 12)):
                        depth["sell" if i >= 5 else "buy"].append({
                            "quantity": self._unpack_int(packet, p, p + 4),
                            "price": self._unpack_int(packet, p + 4, p + 8) / divisor,
                            "orders": self._unpack_int(packet, p + 8, p + 10, byte_format="H")
                        })

                    d["depth"] = depth

                data.append(d)

        return data

    def _unpack_int(self, bin, start, end, byte_format="I"):
        """Unpack binary data as unsgined interger."""
        return struct.unpack(">" + byte_format, bin[start:end])[0]

    def _split_packets(self, bin):
        """Split the data to individual packets of ticks."""
        # Ignore heartbeat data.
        if len(bin) < 2:
            return []

        number_of_packets = self._unpack_int(bin, 0, 2, byte_format="H")
        packets = []

        j = 2
        for i in range(number_of_packets):
            packet_length = self._unpack_int(bin, j, j + 2, byte_format="H")
            packets.append(bin[j + 2: j + 2 + packet_length])
            j = j + 2 + packet_length

        return packets
