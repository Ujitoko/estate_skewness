"""不動産歪みマップ: エントリーポイント

取引価格（実勢価格）と地価公示価格（公的価格）の乖離率を
市区町村レベル（東京都全体）で可視化する。
"""

import logging
import os
import sys

import config
from api_client import ReinfolibClient
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from map_builder import MapBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    if not config.API_KEY:
        logger.error(
            "環境変数 REINFOLIB_API_KEY が設定されていません。\n"
            "APIキー申請先: https://www.reinfolib.mlit.go.jp/ex-api/api_apply.html\n"
            ".env ファイルまたは環境変数に設定してください。"
        )
        sys.exit(1)

    logger.info("=== 不動産歪みマップ生成開始 ===")
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    client = ReinfolibClient()
    fetcher = DataFetcher(client)

    logger.info("--- データ取得 ---")
    municipalities = fetcher.fetch_municipalities()
    transactions = fetcher.fetch_all_transactions(municipalities)
    official_prices = fetcher.fetch_official_prices()
    boundaries = fetcher.fetch_municipality_boundaries()

    if not boundaries or not boundaries.get("features"):
        logger.error("市区町村境界データを取得できませんでした")
        sys.exit(1)

    logger.info("--- 乖離率計算 ---")
    processor = DataProcessor(transactions, official_prices, boundaries)
    results = processor.process()

    logger.info("--- 地図生成 ---")
    builder = MapBuilder(results)
    path = builder.build()

    logger.info("=== 完了 ===")
    logger.info("出力: %s", path)


if __name__ == "__main__":
    main()
