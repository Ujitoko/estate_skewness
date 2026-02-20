"""定数・設定"""

import os
from dotenv import load_dotenv

load_dotenv()

# APIキー
API_KEY = os.environ.get("REINFOLIB_API_KEY", "")

# API基本URL
API_BASE_URL = "https://www.reinfolib.mlit.go.jp/ex-api/external"

# 全国47都道府県コード
PREF_CODES = [f"{i:02d}" for i in range(1, 48)]

# 地域別バウンディングボックス (XPT002タイル走査用)
# 日本列島を地域ごとに分割し、海洋部分のタイル走査を回避する
REGION_BBOXES = [
    {"name": "北海道", "north": 45.60, "south": 41.30, "west": 139.30, "east": 145.90},
    {"name": "東北", "north": 41.60, "south": 36.70, "west": 139.00, "east": 142.10},
    {"name": "関東・甲信", "north": 37.80, "south": 34.80, "west": 138.00, "east": 141.00},
    {"name": "北陸", "north": 38.60, "south": 35.80, "west": 135.80, "east": 140.10},
    {"name": "東海", "north": 35.70, "south": 34.20, "west": 136.40, "east": 139.20},
    {"name": "近畿", "north": 35.80, "south": 33.40, "west": 134.00, "east": 136.90},
    {"name": "中国", "north": 35.70, "south": 33.70, "west": 130.70, "east": 134.50},
    {"name": "四国", "north": 34.40, "south": 32.70, "west": 131.90, "east": 134.90},
    {"name": "九州", "north": 34.30, "south": 30.90, "west": 129.40, "east": 132.10},
    {"name": "南西諸島", "north": 31.00, "south": 27.50, "west": 128.30, "east": 131.50},
    {"name": "沖縄", "north": 27.50, "south": 25.80, "west": 126.50, "east": 128.50},
    {"name": "先島諸島", "north": 25.80, "south": 24.00, "west": 122.90, "east": 126.00},
]

# 取引年範囲 (XIT001は year + quarter)
TRANSACTION_YEARS = [2022, 2023, 2024, 2025]
TRANSACTION_QUARTERS = [1, 2, 3, 4]

# 公示価格年（取引データと同じ範囲）
OFFICIAL_PRICE_YEARS = [2022, 2023, 2024, 2025]

# タイルzoom (XPT002用 ※zoom 13以上のみ対応)
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

# 地図初期中心座標 (日本中心付近)
MAP_CENTER = [36.50, 137.00]
MAP_ZOOM = 6
