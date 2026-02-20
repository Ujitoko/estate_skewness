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
        """XIT002: 全国の市区町村一覧を取得。"""
        cache_key = self._cache_key("municipalities", {"prefs": config.PREF_CODES})
        cached = self._read_cache(cache_key)
        if cached is not None:
            logger.info("市区町村一覧: キャッシュから %d 件", len(cached))
            return cached

        all_data: list[dict] = []
        for pref_code in config.PREF_CODES:
            logger.info("市区町村一覧を取得中: 都道府県コード %s", pref_code)
            resp = self._client.get("XIT002", params={"area": pref_code})
            data = resp.get("data", [])
            logger.info("  %s: %d 件", pref_code, len(data))
            all_data.extend(data)

        logger.info("市区町村一覧合計: %d 件", len(all_data))
        self._write_cache(cache_key, all_data)
        return all_data

    # ---- 取引データ ----

    def _migrate_old_pref_cache(self, pref_code: str) -> None:
        """旧形式（都道府県×全年一括）のキャッシュを年別に分割マイグレーション。"""
        for old_years in [[2022, 2023, 2024]]:
            old_key = self._cache_key(f"transactions_{pref_code}", {
                "pref": pref_code,
                "years": old_years,
                "quarters": config.TRANSACTION_QUARTERS,
            })
            old_data = self._read_cache(old_key)
            if old_data is None:
                continue

            logger.info("キャッシュ移行 [%s]: %d 件を年別に分割", pref_code, len(old_data))
            by_year: dict[int, list[dict]] = {}
            for rec in old_data:
                period = rec.get("Period", "")
                for y in old_years:
                    if str(y) in period:
                        by_year.setdefault(y, []).append(rec)
                        break

            for year, records in by_year.items():
                year_key = self._cache_key(f"tx_{pref_code}_{year}", {
                    "pref": pref_code,
                    "year": year,
                    "quarters": config.TRANSACTION_QUARTERS,
                })
                if self._read_cache(year_key) is None:
                    self._write_cache(year_key, records)
                    logger.info("  %s %d年: %d 件 保存", pref_code, year, len(records))

    def fetch_all_transactions(self, municipalities: list[dict]) -> list[dict]:
        """XIT001: 全市区町村×年×四半期の取引データを取得。

        都道府県×年ごとにキャッシュし、中断後の再開が可能。
        """
        # 全体キャッシュ
        all_cache_key = self._cache_key("transactions_all", {
            "prefs": config.PREF_CODES,
            "years": config.TRANSACTION_YEARS,
            "quarters": config.TRANSACTION_QUARTERS,
        })
        cached = self._read_cache(all_cache_key)
        if cached is not None:
            logger.info("取引データ: キャッシュから %d 件", len(cached))
            return cached

        # 市区町村を都道府県コード別にグループ化
        pref_munis: dict[str, list[dict]] = {}
        for muni in municipalities:
            city_code = muni.get("id", muni.get("code", ""))
            pref_code = str(city_code)[:2]
            pref_munis.setdefault(pref_code, []).append(muni)

        all_records: list[dict] = []
        done_chunks = 0
        total_chunks = sum(
            1 for pc in config.PREF_CODES
            if pref_munis.get(pc)
        ) * len(config.TRANSACTION_YEARS)

        for pref_code in config.PREF_CODES:
            munis = pref_munis.get(pref_code, [])
            if not munis:
                continue

            # 旧キャッシュからマイグレーション
            self._migrate_old_pref_cache(pref_code)

            for year in config.TRANSACTION_YEARS:
                year_cache_key = self._cache_key(f"tx_{pref_code}_{year}", {
                    "pref": pref_code,
                    "year": year,
                    "quarters": config.TRANSACTION_QUARTERS,
                })
                year_cached = self._read_cache(year_cache_key)
                if year_cached is not None:
                    done_chunks += 1
                    logger.info(
                        "[%d/%d] 取引データ [%s] %d年: キャッシュから %d 件",
                        done_chunks, total_chunks, pref_code, year, len(year_cached),
                    )
                    all_records.extend(year_cached)
                    continue

                done_chunks += 1
                logger.info("[%d/%d] 取引データ [%s] %d年: 取得開始", done_chunks, total_chunks, pref_code, year)
                year_records: list[dict] = []
                for muni in munis:
                    city_code = muni.get("id", muni.get("code", ""))
                    city_name = muni.get("name", "")
                    muni_count = 0
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
                            year_records.extend(records)
                            muni_count += len(records)
                        except Exception as e:
                            logger.debug(
                                "%s %dQ%d: %s", city_name, year, quarter, e
                            )
                    if muni_count > 0:
                        logger.info("  %s: %d 件", city_name, muni_count)

                logger.info("取引データ [%s] %d年: %d 件", pref_code, year, len(year_records))
                self._write_cache(year_cache_key, year_records)
                all_records.extend(year_records)

        logger.info("取引データ合計: %d 件", len(all_records))
        self._write_cache(all_cache_key, all_records)
        return all_records

    # ---- 公示価格 ----

    def _scan_tiles_for_region(
        self, region: dict, year: int
    ) -> list[dict]:
        """1地域・1年分のタイル走査を実行。"""
        tiles = get_tiles_for_bbox(
            region["north"], region["south"],
            region["west"], region["east"],
            config.TILE_ZOOM,
        )
        records: list[dict] = []
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
                    records.append(props)
                if i % 500 == 0:
                    logger.info(
                        "  [%s] %d年: %d/%d タイル (%d 件)",
                        region["name"], year, i, len(tiles), len(records),
                    )
            except Exception as e:
                logger.debug("タイル (%d,%d) %d年: %s", x, y, year, e)
        return records

    def fetch_official_prices(self) -> list[dict]:
        """XPT002: 地域別タイル走査で日本全域の公示価格を複数年分取得。

        年×地域ごとにキャッシュし、中断後の再開が可能。
        """
        # 全体キャッシュ
        all_cache_key = self._cache_key("official_prices_all", {
            "years": config.OFFICIAL_PRICE_YEARS,
            "zoom": config.TILE_ZOOM,
            "regions": [r["name"] for r in config.REGION_BBOXES],
        })
        cached = self._read_cache(all_cache_key)
        if cached is not None:
            logger.info("公示価格: キャッシュから %d 件", len(cached))
            return cached

        all_records: list[dict] = []

        for year in config.OFFICIAL_PRICE_YEARS:
            # 年別統合キャッシュ
            year_cache_key = self._cache_key(f"official_prices_{year}", {
                "year": year,
                "zoom": config.TILE_ZOOM,
                "regions": [r["name"] for r in config.REGION_BBOXES],
            })
            year_cached = self._read_cache(year_cache_key)
            if year_cached is not None:
                logger.info("公示価格 %d年: キャッシュから %d 件", year, len(year_cached))
                all_records.extend(year_cached)
                continue

            year_records: list[dict] = []
            for region in config.REGION_BBOXES:
                # 年×地域キャッシュ（中断再開用）
                region_cache_key = self._cache_key(
                    f"official_{year}_{region['name']}", {
                        "year": year,
                        "region": region["name"],
                        "zoom": config.TILE_ZOOM,
                        "bbox": {k: v for k, v in region.items() if k != "name"},
                    },
                )
                region_cached = self._read_cache(region_cache_key)
                if region_cached is not None:
                    logger.info(
                        "公示価格 %d年 [%s]: キャッシュから %d 件",
                        year, region["name"], len(region_cached),
                    )
                    year_records.extend(region_cached)
                    continue

                tiles = get_tiles_for_bbox(
                    region["north"], region["south"],
                    region["west"], region["east"],
                    config.TILE_ZOOM,
                )
                logger.info(
                    "公示価格 %d年 [%s]: %d タイルを走査",
                    year, region["name"], len(tiles),
                )
                region_records = self._scan_tiles_for_region(region, year)
                logger.info(
                    "公示価格 %d年 [%s]: %d 件",
                    year, region["name"], len(region_records),
                )
                self._write_cache(region_cache_key, region_records)
                year_records.extend(region_records)

            # 年の重複排除（地域bbox重複分）
            seen = set()
            deduped: list[dict] = []
            for rec in year_records:
                key = (rec.get("_lat"), rec.get("_lon"), rec.get("_year"),
                       rec.get("u_standard_address_code"))
                if key not in seen:
                    seen.add(key)
                    deduped.append(rec)
            logger.info(
                "公示価格 %d年: %d 件 (重複排除前 %d)",
                year, len(deduped), len(year_records),
            )
            self._write_cache(year_cache_key, deduped)
            all_records.extend(deduped)

        logger.info("公示価格合計: %d 件 (%d年分)", len(all_records), len(config.OFFICIAL_PRICE_YEARS))
        self._write_cache(all_cache_key, all_records)
        return all_records

    # ---- 市区町村境界GeoJSON ----

    def fetch_municipality_boundaries(self) -> dict:
        """市区町村境界GeoJSONを取得（GitHub / ローカル）。

        niiyz/JapanCityGeoJson リポジトリから全国の個別市区町村ファイルを
        ダウンロードし、1つのFeatureCollectionにマージする。
        """
        local_path = os.path.join(config.GEOJSON_DIR, "japan_municipalities.geojson")
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
                logger.info("都道府県 %s: %d ファイル", pref_code, len(files))

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
