"""緯度経度 ↔ タイル座標変換"""

import math


def deg2tile(lat: float, lon: float, zoom: int) -> tuple[int, int]:
    """緯度経度からタイル座標 (x, y) を返す。"""
    lat_rad = math.radians(lat)
    n = 2 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y


def get_tiles_for_bbox(
    north: float, south: float, west: float, east: float, zoom: int
) -> list[tuple[int, int]]:
    """バウンディングボックスを覆うタイル座標のリストを返す。"""
    x_min, y_min = deg2tile(north, west, zoom)  # 北西角
    x_max, y_max = deg2tile(south, east, zoom)  # 南東角
    tiles = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            tiles.append((x, y))
    return tiles
