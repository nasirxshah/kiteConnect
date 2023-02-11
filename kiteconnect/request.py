import logging
import urllib.parse
from typing import Callable

import requests
import requests.adapters
import urllib3

import kiteconnect.exeptions as ex

from .__version__ import __title__, __version__

logger = logging.getLogger(__name__)


class RequestSession:
    kite_header_version = '3'
    _default_root_uri = "https://api.kite.trade"
    _default_timeout = 7

    def __init__(self,
                 apikey: str,
                 access_token: str | None = None,
                 root: str | None = None,
                 debug: bool = False,
                 timeout: int | None = None,
                 proxies=None,
                 pool=None,
                 disable_ssl: bool = False) -> None:

        self.root = root if root else self._default_root_uri
        self.apikey = apikey
        self.access_token = access_token

        self.timeout = timeout if timeout else self._default_timeout
        self.disable_ssl = disable_ssl
        self.proxies = proxies if proxies else {}
        self.debug = debug
        self._session_expiry_hook: Callable | None = None

        self.reqsession = requests.Session()
        if pool:
            reqadapter = requests.adapters.HTTPAdapter(**pool)
            self.reqsession.mount("https://", reqadapter)

        # disable requests SSL warning
        urllib3.disable_warnings()

    @property
    def session_expiry_hook(self) -> Callable | None:
        return self._session_expiry_hook

    @session_expiry_hook.setter
    def session_expiry_hook(self, callback: Callable):
        if not callable(callback):
            raise TypeError("Invalid input type. Only functions are accepted.")

        self._session_expiry_hook = callback

    def _user_agent(self) -> str:
        return (__title__ + "-python/").capitalize() + __version__

    def get(self, route, kwargs=None, params=None) -> requests.Response:
        return self._request("GET", route, kwargs=kwargs, params=params)

    def post(self, route, kwargs=None, params=None, data=None, json=None) -> requests.Response:
        return self._request("POST", route, kwargs=kwargs, params=params, data=data, json=json)

    def put(self,  route, kwargs=None, params=None, data=None, json=None) -> requests.Response:
        return self._request("PUT", route, kwargs=kwargs, params=params, data=data, json=json)

    def delete(self, route, kwargs=None, params=None) -> requests.Response:
        return self._request(route, "DELETE", kwargs=kwargs, params=params)

    def _request(self, method: str, route: str, kwargs: dict | None = None, params: dict | None = None, data: dict | None = None, json: dict | None = None) -> requests.Response:
        # Form a restful URL
        url = urllib.parse.urljoin(
            self.root, route.format(**kwargs) if kwargs else route)

        # Custom headers
        headers = {
            "X-Kite-Version": self.kite_header_version,
            "User-Agent": self._user_agent()
        }

        if self.apikey and self.access_token:
            auth_header = f"{self.apikey}:{self.access_token}"
            headers["Authorization"] = f"token {auth_header}"

        if self.debug:
            logger.debug(f"Request: {method} {url} {params} {headers}")

        try:
            resp = self.reqsession.request(method,
                                           url,
                                           params=params,
                                           data=data,
                                           json=json,
                                           headers=headers,
                                           verify=not self.disable_ssl,
                                           allow_redirects=True,
                                           timeout=self.timeout,
                                           proxies=self.proxies
                                           )

            if self.debug:
                logger.debug(f"Response: {resp.status_code} {resp.content}")
            return resp

        except Exception as e:
            raise e

    def extract_json(self, response: requests.Response):
        if "json" in response.headers["content-type"]:
            try:
                rdata: dict = response.json()

            except ValueError:
                raise ex.DataException(
                    f"Couldn't parse the JSON response received from the server: {response.content}")

            # api error
            if rdata.get("status") == "error" or rdata.get("error_type"):
                # Call session hook if its registered and TokenException is raised
                if self.session_expiry_hook and response.status_code == 403 and rdata["error_type"] == "TokenException":
                    self.session_expiry_hook()

                # native Kite errors
                exp = getattr(ex, rdata["error_type"], ex.GeneralException)
                raise exp(rdata["message"], code=response.status_code)

            return rdata["data"]
        else:
            raise ex.DataException(
                f"Unknown Content-Type ({response.headers['content-type']}) with response: ({response.content})")

    def extract_csv(self, response: requests.Response):
        if "csv" in response.headers["content-type"]:
            return response.content

        else:
            raise ex.DataException(
                f"Unknown Content-Type ({response.headers['content-type']}) with response: ({response.content})")
