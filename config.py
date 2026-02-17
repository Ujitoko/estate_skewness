"""定数・設定"""

import os
from dotenv import load_dotenv

load_dotenv()

# APIキー
API_KEY = os.environ.get("REINFOLIB_API_KEY", "")

# API基本URL
API_BASE_URL = "https://www.reinfolib.mlit.go.jp/ex-api/external"

# 関東7県コード
PREF_CODES = ["08", "09", "10", "11", "12", "13", "14"]

# 関東全域バウンディングボックス (XPT002タイル走査用)
REGION_BBOX = {
    "north": 37.00,
    "south": 34.90,
    "west": 138.40,
    "east": 140.90,
}

# 取引年範囲 (XIT001は year + quarter)
TRANSACTION_YEARS = [2022, 2023, 2024]
TRANSACTION_QUARTERS = [1, 2, 3, 4]

# 公示価格年（取引データと同じ範囲）
OFFICIAL_PRICE_YEARS = [2022, 2023, 2024]

# タイルzoom (XPT002用)
TILE_ZOOM = 13

# レート制限 (秒)
REQUEST_INTERVAL = 0.5

# キャッシュディレクトリ
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")

# GeoJSONディレクトリ
GEOJSON_DIR = os.path.join(os.path.dirname(__file__), "geojson")

# 出力ディレクトリ
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")

# 出力ファイル名
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "distortion_map.html")

# 市区町村境界GeoJSON URL (フォールバック用)
MUNICIPALITY_GEOJSON_URL = (
    "https://raw.githubusercontent.com/niiyz/JapanCityGeoJson/master/geojson/custom/tokyo23.json"
)

# 地図初期中心座標 (関東中心付近)
MAP_CENTER = [36.05, 139.65]
MAP_ZOOM = 8
