"""folium地図生成"""

import json
import logging

import branca.colormap as cm
import folium
from folium.plugins import GroupedLayerControl
import geopandas as gpd
import numpy as np

import config

logger = logging.getLogger(__name__)


class MapBuilder:
    """乖離率データからインタラクティブ地図HTMLを生成。

    取引タイプ別のレイヤーを持ち、LayerControl で切替可能。
    """

    # レイヤー表示名
    LAYER_LABELS = {
        "land_only": "宅地(土地のみ)",
        "land_building": "宅地(土地と建物)",
    }

    def __init__(self, results: dict[str, gpd.GeoDataFrame]):
        self._results = results

    def build(self, output_path: str | None = None) -> str:
        output_path = output_path or config.OUTPUT_FILE
        m = self._create_map()
        m.save(output_path)
        logger.info("地図を保存: %s", output_path)
        return output_path

    def _create_map(self) -> folium.Map:
        m = folium.Map(
            location=config.MAP_CENTER,
            zoom_start=config.MAP_ZOOM,
            tiles="CartoDB positron",
        )

        # 全レイヤーの乖離率を集めて共通カラースケールを構築
        all_devs = []
        for gdf in self._results.values():
            vals = gdf["deviation_pct"].dropna()
            if not vals.empty:
                all_devs.extend(vals.tolist())

        if not all_devs:
            logger.warning("有効な乖離率データがありません")
            return m

        abs_max = max(abs(min(all_devs)), abs(max(all_devs)), 10)
        vmin = -abs_max
        vmax = abs_max

        colormap = cm.LinearColormap(
            colors=["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"],
            index=[vmin, vmin / 2, 0, vmax / 2, vmax],
            vmin=vmin,
            vmax=vmax,
            caption="乖離率 (%) ← 割安 | 過熱 →",
        )
        colormap.add_to(m)

        # レイヤーを追加
        for type_key, gdf in self._results.items():
            layer_name = self.LAYER_LABELS.get(type_key, type_key)
            self._add_layer(m, gdf, colormap, layer_name, show=True)

        # タイトル
        title_html = """
        <div style="position: fixed; top: 10px; left: 50px; z-index: 1000;
                    background: white; padding: 10px 20px; border-radius: 5px;
                    border: 2px solid #333; font-size: 16px; font-weight: bold;">
            不動産歪みマップ: 取引価格 vs 公示価格 乖離率
        </div>
        """
        m.get_root().html.add_child(folium.Element(title_html))

        return m

    def _add_layer(
        self,
        m: folium.Map,
        gdf: gpd.GeoDataFrame,
        colormap: cm.LinearColormap,
        layer_name: str,
        show: bool = True,
    ) -> folium.FeatureGroup:
        geojson_data = json.loads(gdf.to_json())

        # NaN → None
        for feature in geojson_data["features"]:
            props = feature["properties"]
            for key, val in props.items():
                if isinstance(val, float) and np.isnan(val):
                    props[key] = None

        def style_function(feature):
            dev = feature["properties"].get("deviation_pct")
            if dev is None:
                return {
                    "fillColor": "#cccccc",
                    "color": "#666666",
                    "weight": 1,
                    "fillOpacity": 0.3,
                }
            return {
                "fillColor": colormap(dev),
                "color": "#333333",
                "weight": 1,
                "fillOpacity": 0.7,
            }

        def highlight_function(feature):
            return {"weight": 3, "color": "#000000", "fillOpacity": 0.85}

        name_col = "city_name_geo"

        fg = folium.FeatureGroup(name=layer_name, show=show)
        geojson_layer = folium.GeoJson(
            geojson_data,
            style_function=style_function,
            highlight_function=highlight_function,
            tooltip=folium.GeoJsonTooltip(
                fields=[name_col, "deviation_pct", "tx_median", "op_mean", "tx_count", "op_count"],
                aliases=["市区町村", "乖離率(%)", "取引中央値(円/㎡)", "公示平均(円/㎡)", "取引件数", "公示地点数"],
                localize=True,
                sticky=True,
                labels=True,
                style="""
                    background-color: white;
                    border: 2px solid black;
                    border-radius: 3px;
                    box-shadow: 3px 3px 3px rgba(0,0,0,0.3);
                    font-size: 14px;
                    padding: 8px;
                """,
            ),
            popup=folium.GeoJsonPopup(
                fields=[
                    name_col, "deviation_pct",
                    "tx_median", "tx_count",
                    "op_mean", "op_count",
                ],
                aliases=[
                    "市区町村", "乖離率 (%)",
                    "取引㎡単価中央値 (円)", "取引件数",
                    "公示価格平均 (円/㎡)", "公示地点数",
                ],
                localize=True,
                labels=True,
                style="font-size: 13px;",
            ),
        )
        geojson_layer.add_to(fg)
        fg.add_to(m)
        return fg
