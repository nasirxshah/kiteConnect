import hashlib
from kiteconnect.request import RequestSession
import dateutil.parser
from kiteconnect.routes import Route


class KiteSession:
    kite_header_version = '3'
    _default_login_uri = "https://kite.zerodha.com/connect/login"

    def __init__(self, reqSession: RequestSession) -> None:
        self.reqSession = reqSession

    def login_url(self):
        """Get the remote login url to which a user should be redirected to initiate the login flow."""
        return f"{self._default_login_uri}?api_key={self.reqSession.apikey}&v={self.kite_header_version}"

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
        h = hashlib.sha256(self.reqSession.apikey.encode(
            "utf-8") + request_token.encode("utf-8") + api_secret.encode("utf-8"))
        checksum = h.hexdigest()

        resp = self.reqSession.post(Route.API_TOKEN, data={
            "api_key": self.reqSession.apikey,
            "request_token": request_token,
            "checksum": checksum
        })
        data = self.reqSession.extract_json(response=resp)

        if data.get("access_token"):
            self.reqSession.access_token = data["access_token"]

        if data.get("login_time") and len(data["login_time"]) == 19:
            data["login_time"] = dateutil.parser.parse(data["login_time"])

        return data

    def invalidate_access_token(self, access_token=None):
        """
        Kill the session by invalidating the access token.

        - `access_token` to invalidate. Default is the active `access_token`.
        """
        access_token = access_token if access_token else self.reqSession.access_token
        resp = self.reqSession.delete(Route.API_TOKEN_INVALIDATE, params={
            "api_key": self.reqSession.apikey,
            "access_token": access_token
        })
        return self.reqSession.extract_json(response=resp)

    def renew_access_token(self, refresh_token, api_secret):
        """
        Renew expired `refresh_token` using valid `refresh_token`.

        - `refresh_token` is the token obtained from previous successful login flow.
        - `api_secret` is the API api_secret issued with the API key.
        """
        h = hashlib.sha256(self.reqSession.apikey.encode(
            "utf-8") + refresh_token.encode("utf-8") + api_secret.encode("utf-8"))
        checksum = h.hexdigest()

        resp = self.reqSession.post(Route.API_TOKEN_RENEW, params={
            "api_key": self.reqSession.apikey,
            "refresh_token": refresh_token,
            "checksum": checksum
        })
        data = self.reqSession.extract_json(response=resp)

        if data.get("access_token"):
            self.reqSession.access_token = data["access_token"]

        return data

    def invalidate_refresh_token(self, refresh_token):
        """
        Invalidate refresh token.

        - `refresh_token` is the token which is used to renew access token.
        """
        resp = self.reqSession.delete(Route.API_TOKEN_INVALIDATE, params={
            "api_key": self.reqSession.apikey,
            "refresh_token": refresh_token
        })
        return self.reqSession.extract_json(response=resp)
