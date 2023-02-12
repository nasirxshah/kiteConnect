import hashlib
from kiteconnect.request import RequestSession
import dateutil.parser
from kiteconnect.routes import Route
from typing import Callable


class KiteSession:
    kite_header_version = '3'
    _default_login_uri = "https://kite.zerodha.com/connect/login"
    _default_root_uri = "https://api.kite.trade"

    def __init__(self, apikey: str) -> None:
        self._apikey: str = apikey
        self._access_token: str | None = None
        self._refresh_token: str | None = None

        self.reqSession = RequestSession(root=self._default_root_uri)
        self.reqSession.apikey = self.apikey
        self.reqSession.access_token = self.access_token

    @property
    def apikey(self):
        return self._apikey

    @property
    def access_token(self):
        return self._access_token

    def login_url(self):
        """Get the remote login url to which a user should be redirected to initiate the login flow."""
        login_url = f"{self._default_login_uri}?api_key={self.apikey}&v={self.kite_header_version}"

    def generate_session(self, request_token, api_secret):
        """
        Generate user session details like `access_token` etc by exchanging `request_token`.
        Access token is automatically set if the session is retrieved successfully.

        Do the token exchange with the `request_token` obtained after the login flow,
        and retrieve the `access_token` required for all subsequent requests. The
        response contains not just the `access_token`, but metadata for
        the user who has authenticated.

        - `request_token` is the token obtained from the GET paramers after a successful login redirect.
        - `api_secret` is the API api_secret issued with the API key.
        """
        _secret = self.apikey.encode() + request_token.encode() + \
            api_secret.encode("utf-8")
        checksum = hashlib.sha256(_secret).hexdigest()

        data = self.reqSession.post(Route.API_TOKEN, data={
            "api_key": self.apikey,
            "request_token": request_token,
            "checksum": checksum
        })

        if data.get("access_token"):
            self._access_token = data["access_token"]

        if data.get("login_time") and len(data["login_time"]) == 19:
            data["login_time"] = dateutil.parser.parse(data["login_time"])

        if data.get("access_token"):
            self._access_token = data["access_token"]

        return data

    def invalidate_access_token(self):
        """
        Kill the session by invalidating the access token.

        - `access_token` to invalidate. Default is the active `access_token`.
        """
        data = self.reqSession.delete(Route.API_TOKEN_INVALIDATE, params={
            "api_key": self.apikey,
            "access_token": self._access_token
        })
        self._access_token = None
        return data

    def renew_access_token(self, api_secret):
        """
        Renew expired `refresh_token` using valid `refresh_token`.

        - `refresh_token` is the token obtained from previous successful login flow.
        - `api_secret` is the API api_secret issued with the API key.
        """
        assert self._refresh_token is not None
        _secret = self.apikey.encode() + self._refresh_token.encode() + \
            api_secret.encode("utf-8")
        checksum = hashlib.sha256(_secret).hexdigest()

        data = self.reqSession.post(Route.API_TOKEN_RENEW, params={
            "api_key": self.apikey,
            "refresh_token": self._refresh_token,
            "checksum": checksum
        })

        if data.get("access_token"):
            self._access_token = data["access_token"]

        return data

    def invalidate_refresh_token(self):
        """
        Invalidate refresh token.

        - `refresh_token` is the token which is used to renew access token.
        """
        assert self._refresh_token is not None
        data = self.reqSession.delete(Route.API_TOKEN_INVALIDATE, params={
            "api_key": self.apikey,
            "refresh_token": self._refresh_token
        })

        self._refresh_token = None
        return data
