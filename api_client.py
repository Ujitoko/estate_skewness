"""HTTPクライアント（リトライ・レート制限）"""

import logging
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config

logger = logging.getLogger(__name__)


class ReinfolibClient:
    """不動産情報ライブラリAPI用HTTPクライアント。"""

    def __init__(self, api_key: str | None = None):
        self._api_key = api_key or config.API_KEY
        if not self._api_key:
            raise ValueError(
                "APIキーが設定されていません。環境変数 REINFOLIB_API_KEY を設定してください。"
            )
        self._last_request_time = 0.0
        self._session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            "Ocp-Apim-Subscription-Key": self._api_key,
        })
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_time
        wait = config.REQUEST_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        """JSON APIエンドポイントを呼び出す。"""
        url = f"{config.API_BASE_URL}/{endpoint}"
        self._throttle()
        logger.debug("GET %s params=%s", url, params)
        resp = self._session.get(url, params=params, timeout=30)
        self._last_request_time = time.time()
        resp.raise_for_status()
        return resp.json()

    def get_geojson(self, endpoint: str, params: dict | None = None) -> dict:
        """GeoJSON APIエンドポイントを呼び出す。"""
        return self.get(endpoint, params)
