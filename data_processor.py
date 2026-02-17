"""データ加工・乖離率計算"""

import logging

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

import config

logger = logging.getLogger(__name__)

# 取引タイプ定義
TRANSACTION_TYPES = {
    "land_only": {
        "label": "宅地(土地)",
        "filter": lambda t: t == "宅地(土地)",
    },
}


class DataProcessor:
    """取引・公示データを加工し、市区町村ごとの乖離率を算出。"""

    def __init__(
        self,
        transactions: list[dict],
        official_prices: list[dict],
        boundaries_geojson: dict,
    ):
        self._raw_transactions = transactions
        self._raw_official = official_prices
        self._boundaries = boundaries_geojson

    def process(self) -> dict[str, gpd.GeoDataFrame]:
        """全処理を実行し、取引タイプ別の乖離率付き GeoDataFrame を返す。"""
        tx_df = self._clean_transactions()
        op_df = self._clean_official_prices()
        gdf = self._load_boundaries()
        op_stats = self._compute_official_stats(op_df, gdf)

        results = {}
        for type_key, type_def in TRANSACTION_TYPES.items():
            label = type_def["label"]
            filtered = tx_df[tx_df["Type"].apply(type_def["filter"])].copy()
            logger.info("%s: %d 件", label, len(filtered))
            result = self._compute_deviation_ratios(
                filtered, op_stats, gdf, label
            )
            results[type_key] = result

        return results

    # ---- 取引データのクリーニング ----

    def _clean_transactions(self) -> pd.DataFrame:
        if not self._raw_transactions:
            logger.warning("取引データが空です")
            return pd.DataFrame()

        df = pd.DataFrame(self._raw_transactions)
        logger.info("取引データ元件数: %d", len(df))

        # 宅地を含むもののみ残す (Type カラムは後でタイプ別フィルタに使う)
        if "Type" in df.columns:
            df = df[df["Type"].str.contains("宅地", na=False)].copy()
            logger.info("宅地フィルタ後: %d 件", len(df))

        # 単価計算
        df["price"] = pd.to_numeric(
            df.get("TradePrice", df.get("PricePerUnit", pd.Series(dtype=float))),
            errors="coerce",
        )
        df["area"] = pd.to_numeric(
            df.get("Area", pd.Series(dtype=float)), errors="coerce"
        )

        mask = df["area"].notna() & (df["area"] > 0)
        df.loc[mask, "price_per_sqm"] = df.loc[mask, "price"] / df.loc[mask, "area"]
        df.loc[~mask, "price_per_sqm"] = df.loc[~mask, "price"]

        # 市区町村コード
        if "_city_code" in df.columns:
            df["city_code"] = df["_city_code"].astype(str)
        elif "MunicipalityCode" in df.columns:
            df["city_code"] = df["MunicipalityCode"].astype(str)
        else:
            df["city_code"] = ""

        df = df.dropna(subset=["price_per_sqm"])
        df = df[df["price_per_sqm"] > 0]
        logger.info("有効な取引データ: %d 件", len(df))
        return df

    # ---- 公示価格のクリーニング ----

    def _clean_official_prices(self) -> pd.DataFrame:
        if not self._raw_official:
            logger.warning("公示価格データが空です")
            return pd.DataFrame()

        df = pd.DataFrame(self._raw_official)
        logger.info("公示価格元件数: %d", len(df))

        # 関東7県フィルタ
        if "prefecture_code" in df.columns:
            df["prefecture_code"] = df["prefecture_code"].astype(str)
            before = len(df)
            df = df[df["prefecture_code"].isin(config.PREF_CODES)].copy()
            logger.info("関東フィルタ: %d → %d 件", before, len(df))

        # 住宅地のみフィルタ
        if "use_category_name_ja" in df.columns:
            before = len(df)
            df = df[df["use_category_name_ja"] == "住宅地"].copy()
            logger.info("住宅地フィルタ: %d → %d 件", before, len(df))

        # 価格パース
        if "u_current_years_price_ja" in df.columns:
            price_str = df["u_current_years_price_ja"].astype(str)
            is_sqm = price_str.str.contains("㎡", na=False)
            before_filter = len(df)
            df = df[is_sqm].copy()
            logger.info("㎡単価のみフィルタ: %d → %d 件", before_filter, len(df))
            df["official_price"] = (
                df["u_current_years_price_ja"]
                .astype(str)
                .str.extract(r"^([\d,]+)", expand=False)
                .str.replace(",", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
            )
        elif "last_years_price" in df.columns:
            df["official_price"] = pd.to_numeric(
                df["last_years_price"], errors="coerce"
            )
        else:
            for col in ["currencyAsOfLandPrice", "price", "Price"]:
                if col in df.columns:
                    df["official_price"] = pd.to_numeric(df[col], errors="coerce")
                    break
            else:
                df["official_price"] = pd.NA

        df["lat"] = pd.to_numeric(df.get("_lat", pd.Series(dtype=float)), errors="coerce")
        df["lon"] = pd.to_numeric(df.get("_lon", pd.Series(dtype=float)), errors="coerce")
        df = df.dropna(subset=["official_price", "lat", "lon"])
        df = df[df["official_price"] > 0]
        logger.info("有効な公示価格: %d 件", len(df))
        return df

    # ---- 境界データ読み込み ----

    def _load_boundaries(self) -> gpd.GeoDataFrame:
        gdf = gpd.GeoDataFrame.from_features(
            self._boundaries["features"], crs="EPSG:4326"
        )
        if "city_code" not in gdf.columns:
            for col in ["N03_007", "code", "id", "cityCode"]:
                if col in gdf.columns:
                    gdf["city_code"] = gdf[col].astype(str)
                    break
        gdf["city_code"] = gdf["city_code"].astype(str)
        for col in ["N03_004", "name", "cityName", "nam"]:
            if col in gdf.columns:
                gdf["city_name_geo"] = gdf[col].astype(str)
                break
        logger.info("境界データ: %d 地域", len(gdf))
        return gdf

    # ---- 公示価格の市区町村別集計 (共通) ----

    def _compute_official_stats(
        self, op_df: pd.DataFrame, gdf: gpd.GeoDataFrame
    ) -> pd.DataFrame:
        """公示価格を空間結合で市区町村に割当て、平均・件数を算出。"""
        if op_df.empty:
            return pd.DataFrame(columns=["city_code", "op_mean", "op_count"])

        points = gpd.GeoDataFrame(
            op_df[["official_price"]].copy(),
            geometry=[Point(xy) for xy in zip(op_df["lon"], op_df["lat"])],
            crs="EPSG:4326",
        )
        boundary_slim = gdf[["city_code", "geometry"]].copy()
        boundary_slim = boundary_slim.rename(columns={"city_code": "geo_city_code"})
        joined = gpd.sjoin(points, boundary_slim, how="left", predicate="within")
        op_stats = (
            joined.groupby("geo_city_code")["official_price"]
            .agg(["mean", "count"])
            .reset_index()
        )
        op_stats.columns = ["city_code", "op_mean", "op_count"]
        logger.info("公示統計: %d 市区町村", len(op_stats))
        return op_stats

    # ---- 乖離率計算 ----

    def _compute_deviation_ratios(
        self,
        tx_df: pd.DataFrame,
        op_stats: pd.DataFrame,
        gdf: gpd.GeoDataFrame,
        label: str,
    ) -> gpd.GeoDataFrame:
        # 取引中央値
        if tx_df.empty:
            tx_stats = pd.DataFrame(columns=["city_code", "tx_median", "tx_count"])
        else:
            tx_stats = (
                tx_df.groupby("city_code")["price_per_sqm"]
                .agg(["median", "count"])
                .reset_index()
            )
            tx_stats.columns = ["city_code", "tx_median", "tx_count"]

        result = gdf[["city_code", "city_name_geo", "geometry"]].copy()
        result = result.merge(tx_stats, on="city_code", how="left")
        result = result.merge(op_stats, on="city_code", how="left")

        # 乖離率
        mask = result["op_mean"].notna() & (result["op_mean"] > 0)
        result.loc[mask, "deviation_pct"] = (
            (result.loc[mask, "tx_median"] - result.loc[mask, "op_mean"])
            / result.loc[mask, "op_mean"]
            * 100
        )

        valid = result["deviation_pct"].notna().sum()
        logger.info(
            "[%s] 乖離率算出: %d / %d 市区町村", label, valid, len(result)
        )
        if valid > 0:
            logger.info(
                "[%s] min=%.1f%%, max=%.1f%%, median=%.1f%%",
                label,
                result["deviation_pct"].min(),
                result["deviation_pct"].max(),
                result["deviation_pct"].median(),
            )

        return result
