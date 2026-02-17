"""API呼び出し・キャッシュ管理"""

import hashlib
import json
import logging
import os
from pathlib import Path

import requests

import config
from api_client import ReinfolibClient
from tile_utils import get_tiles_for_bbox

logger = logging.getLogger(__name__)


class DataFetcher:
    """APIデータ取得 + JSONファイルキャッシュ。"""

    def __init__(self, client: ReinfolibClient):
        self._client = client
        os.makedirs(config.CACHE_DIR, exist_ok=True)
        os.makedirs(config.GEOJSON_DIR, exist_ok=True)

    # ---- キャッシュ ----

    @staticmethod
    def _cache_key(prefix: str, params: dict) -> str:
        raw = json.dumps(params, sort_keys=True)
        h = hashlib.md5(raw.encode()).hexdigest()[:12]
        return f"{prefix}_{h}"

    def _read_cache(self, key: str) -> dict | list | None:
        path = os.path.join(config.CACHE_DIR, f"{key}.json")
        if os.path.exists(path):
            logger.debug("キャッシュヒット: %s", key)
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def _write_cache(self, key: str, data) -> None:
        path = os.path.join(config.CACHE_DIR, f"{key}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    # ---- 市区町村一覧 ----

    def fetch_municipalities(self) -> list[dict]:
        """XIT002: 関東全県の市区町村一覧を取得。"""
        cache_key = self._cache_key("municipalities", {"prefs": config.PREF_CODES})
        cached = self._read_cache(cache_key)
        if cached is not None:
            logger.info("市区町村一覧: キャッシュから %d 件", len(cached))
            return cached

        all_data: list[dict] = []
        for pref_code in config.PREF_CODES:
            logger.info("市区町村一覧を取得中: 都県コード %s", pref_code)
            resp = self._client.get("XIT002", params={"area": pref_code})
            data = resp.get("data", [])
            logger.info("  %s: %d 件", pref_code, len(data))
            all_data.extend(data)

        logger.info("市区町村一覧合計: %d 件", len(all_data))
        self._write_cache(cache_key, all_data)
        return all_data

    # ---- 取引データ ----

    def fetch_all_transactions(self, municipalities: list[dict]) -> list[dict]:
        """XIT001: 全市区町村×年×四半期の取引データを取得。

        XIT001 は city (5桁市区町村コード) + year (YYYY) + quarter (1-4) を使う。
        area パラメータは都道府県コード(2桁)のみ受け付ける。
        """
        cache_key = self._cache_key("transactions_all", {
            "prefs": config.PREF_CODES,
            "years": config.TRANSACTION_YEARS,
            "quarters": config.TRANSACTION_QUARTERS,
        })
        cached = self._read_cache(cache_key)
        if cached is not None:
            logger.info("取引データ: キャッシュから %d 件", len(cached))
            return cached

        all_records: list[dict] = []
        total = len(municipalities)
        for i, muni in enumerate(municipalities, 1):
            city_code = muni.get("id", muni.get("code", ""))
            city_name = muni.get("name", "")
            muni_count = 0
            for year in config.TRANSACTION_YEARS:
                for quarter in config.TRANSACTION_QUARTERS:
                    params = {
                        "city": city_code,
                        "year": year,
                        "quarter": quarter,
                    }
                    try:
                        resp = self._client.get("XIT001", params=params)
                        records = resp.get("data", [])
                        for r in records:
                            r["_city_code"] = city_code
                            r["_city_name"] = city_name
                        all_records.extend(records)
                        muni_count += len(records)
                    except Exception as e:
                        logger.debug(
                            "%s %dQ%d: %s", city_name, year, quarter, e
                        )
            logger.info("[%d/%d] %s: %d 件", i, total, city_name, muni_count)

        logger.info("取引データ合計: %d 件", len(all_records))
        self._write_cache(cache_key, all_records)
        return all_records

    # ---- 公示価格 ----

    def fetch_official_prices(self) -> list[dict]:
        """XPT002: タイル走査で関東全域の公示価格を複数年分取得。"""
        cache_key = self._cache_key("official_prices", {
            "years": config.OFFICIAL_PRICE_YEARS,
            "zoom": config.TILE_ZOOM,
            "bbox": config.REGION_BBOX,
        })
        cached = self._read_cache(cache_key)
        if cached is not None:
            logger.info("公示価格: キャッシュから %d 件", len(cached))
            return cached

        tiles = get_tiles_for_bbox(
            config.REGION_BBOX["north"],
            config.REGION_BBOX["south"],
            config.REGION_BBOX["west"],
            config.REGION_BBOX["east"],
            config.TILE_ZOOM,
        )

        all_records: list[dict] = []
        for year in config.OFFICIAL_PRICE_YEARS:
            logger.info("公示価格 %d年: %d タイルを走査", year, len(tiles))
            year_count = 0
            for i, (x, y) in enumerate(tiles, 1):
                params = {
                    "response_format": "geojson",
                    "year": year,
                    "z": config.TILE_ZOOM,
                    "x": x,
                    "y": y,
                    "priceClassification": 1,
                }
                try:
                    resp = self._client.get_geojson("XPT002", params=params)
                    features = resp.get("features", [])
                    for f in features:
                        props = f.get("properties", {})
                        geom = f.get("geometry", {})
                        coords = geom.get("coordinates", [None, None])
                        props["_lon"] = coords[0]
                        props["_lat"] = coords[1]
                        props["_year"] = year
                        all_records.append(props)
                    year_count += len(features)
                    if i % 50 == 0:
                        logger.info(
                            "公示価格 %d年: %d/%d タイル完了 (%d 件)",
                            year, i, len(tiles), year_count,
                        )
                except Exception as e:
                    logger.debug("タイル (%d,%d) %d年: %s", x, y, year, e)
            logger.info("公示価格 %d年: %d 件", year, year_count)

        logger.info("公示価格合計: %d 件 (%d年分)", len(all_records), len(config.OFFICIAL_PRICE_YEARS))
        self._write_cache(cache_key, all_records)
        return all_records

    # ---- 市区町村境界GeoJSON ----

    def fetch_municipality_boundaries(self) -> dict:
        """市区町村境界GeoJSONを取得（GitHub / ローカル）。

        niiyz/JapanCityGeoJson リポジトリから関東7県の個別市区町村ファイルを
        ダウンロードし、1つのFeatureCollectionにマージする。
        """
        local_path = os.path.join(config.GEOJSON_DIR, "kanto_municipalities.geojson")
        if os.path.exists(local_path):
            logger.info("境界GeoJSON: ローカルから読み込み")
            with open(local_path, "r", encoding="utf-8") as f:
                return json.load(f)

        logger.info("境界GeoJSON: ダウンロード中...")

        all_features: list[dict] = []
        for pref_code in config.PREF_CODES:
            base_url = (
                f"https://raw.githubusercontent.com/niiyz/JapanCityGeoJson/"
                f"master/geojson/{pref_code}"
            )
            api_url = (
                f"https://api.github.com/repos/niiyz/JapanCityGeoJson/"
                f"contents/geojson/{pref_code}"
            )
            try:
                list_resp = requests.get(api_url, timeout=30)
                list_resp.raise_for_status()
                files = [f["name"] for f in list_resp.json() if f["name"].endswith(".json")]
                logger.info("都県 %s: %d ファイル", pref_code, len(files))

                for i, fname in enumerate(files, 1):
                    url = f"{base_url}/{fname}"
                    try:
                        data = self._download_geojson(url)
                        if data and data.get("features"):
                            code = fname.replace(".json", "")
                            for feat in data["features"]:
                                feat["properties"]["city_code"] = code
                            all_features.extend(data["features"])
                    except Exception as e:
                        logger.debug("GeoJSON取得失敗 %s: %s", fname, e)
                    if i % 20 == 0:
                        logger.info("  %s: %d/%d ファイル取得", pref_code, i, len(files))
            except Exception as e:
                logger.warning("GitHub APIからの一覧取得失敗 (%s): %s", pref_code, e)

        if not all_features:
            logger.error("境界GeoJSONを取得できませんでした")
            return {"type": "FeatureCollection", "features": []}

        geojson_data = {"type": "FeatureCollection", "features": all_features}

        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, ensure_ascii=False)
        logger.info("境界GeoJSON: %d 地域を保存", len(all_features))

        return geojson_data

    @staticmethod
    def _download_geojson(url: str) -> dict | None:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
