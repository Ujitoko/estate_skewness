"""folium地図生成"""

import json
import logging

import branca.colormap as cm
import folium
from folium.plugins import GroupedLayerControl
import geopandas as gpd
import numpy as np
import topojson as tp

import config

logger = logging.getLogger(__name__)


class MapBuilder:
    """乖離率データからインタラクティブ地図HTMLを生成。

    乖離率・取引中央値・公示中央値の3指標をラジオボタンで切替可能。
    """

    def __init__(self, results: dict[str, gpd.GeoDataFrame]):
        self._results = results

    def build(self, output_path: str | None = None) -> str:
        output_path = output_path or config.OUTPUT_FILE
        m = self._create_map()
        m.save(output_path)
        logger.info("地図を保存: %s", output_path)
        return output_path

    def _simplify(self, gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, dict]:
        """TopoJSON経由で簡略化し、GeoJSON dictも返す（1回だけ実行）。"""
        gdf = gdf.copy()
        topo = tp.Topology(gdf, toposimplify=0.001)
        gdf = topo.to_gdf()
        geojson_data = json.loads(gdf.to_json(na="null"))

        # 座標精度を4桁(約11m)に丸めてファイルサイズ削減
        def _round_coords(obj):
            if isinstance(obj, list):
                if obj and isinstance(obj[0], (int, float)):
                    return [round(v, 4) for v in obj]
                return [_round_coords(item) for item in obj]
            return obj

        for feature in geojson_data["features"]:
            geom = feature.get("geometry", {})
            if "coordinates" in geom:
                geom["coordinates"] = _round_coords(geom["coordinates"])
            props = feature["properties"]
            for key, val in props.items():
                if isinstance(val, float) and np.isnan(val):
                    props[key] = None

        return gdf, geojson_data

    def _create_map(self) -> folium.Map:
        m = folium.Map(
            location=config.MAP_CENTER,
            zoom_start=config.MAP_ZOOM,
            tiles="CartoDB positron",
        )

        # land_only のデータを使う
        gdf = self._results.get("land_only")
        if gdf is None:
            gdf = next(iter(self._results.values()))

        valid_devs = gdf["deviation_pct"].dropna()
        if valid_devs.empty:
            logger.warning("有効な乖離率データがありません")
            return m

        # 万円カラムを追加
        gdf["tx_median_man"] = (gdf["tx_median"] / 10000).round(1)
        gdf["op_median_man"] = (gdf["op_median"] / 10000).round(1)

        # ジオメトリ簡略化 (1回だけ)
        gdf, geojson_data = self._simplify(gdf)

        # --- 3つの指標のカラーマップ定義 ---
        def _nice_ceil(v):
            """値をきりの良い上限に丸める (例: 18.9→20, 4.7→5, 123→150)"""
            if v <= 0:
                return 1
            import math
            exp = math.floor(math.log10(v))
            base = 10 ** exp
            nice_steps = [1, 1.5, 2, 3, 5, 7.5, 10]
            for s in nice_steps:
                candidate = s * base
                if candidate >= v:
                    return candidate
            return 10 * base

        tx_q95 = _nice_ceil(float(gdf["tx_median_man"].dropna().quantile(0.95)))
        op_q95 = _nice_ceil(float(gdf["op_median_man"].dropna().quantile(0.95)))

        layers = [
            {
                "name": "乖離率 (%)",
                "field": "deviation_pct",
                "vmin": -100,
                "vmax": 100,
                "colors": ["#2166ac", "#67a9cf", "#f7f7f7", "#ef8a62", "#b2182b"],
                "caption": "乖離率 (%) ← 割安 | 過熱 →",
                "show": True,
            },
            {
                "name": "取引中央値 (万円/㎡)",
                "field": "tx_median_man",
                "vmin": 0,
                "vmax": tx_q95,
                "colors": ["#ffffcc", "#a1dab4", "#41b6c4", "#2c7fb8", "#253494"],
                "caption": "取引㎡単価中央値 (万円/㎡)",
                "show": False,
            },
            {
                "name": "公示中央値 (万円/㎡)",
                "field": "op_median_man",
                "vmin": 0,
                "vmax": op_q95,
                "colors": ["#ffffcc", "#a1dab4", "#41b6c4", "#2c7fb8", "#253494"],
                "caption": "公示価格中央値 (万円/㎡)",
                "show": False,
            },
        ]

        groups = {"指標切替": []}
        legend_ids = []

        for i, layer_def in enumerate(layers):
            vmin, vmax = layer_def["vmin"], layer_def["vmax"]
            field = layer_def["field"]
            legend_id = f"legend-{i}"
            legend_ids.append(legend_id)

            n_colors = len(layer_def["colors"])
            tick_step = (vmax - vmin) / (n_colors - 1)
            index = [round(vmin + tick_step * i, 1) for i in range(n_colors)]
            colormap = cm.LinearColormap(
                colors=layer_def["colors"],
                index=index,
                vmin=vmin,
                vmax=vmax,
                caption=layer_def["caption"],
            )

            # カラーバーを白背景のdivに格納し、初期表示/非表示を設定
            display = "block" if layer_def["show"] else "none"
            legend_html = colormap._repr_html_()
            wrapped = f"""
            <div id="{legend_id}" style="
                display: {display};
                position: fixed; bottom: 20px; left: 20px; z-index: 1000;
                background: white; padding: 10px 14px; border-radius: 5px;
                border: 2px solid #333; box-shadow: 3px 3px 6px rgba(0,0,0,0.3);
            ">{legend_html}</div>
            """
            m.get_root().html.add_child(folium.Element(wrapped))

            def make_style_fn(cmap, fld, lo, hi):
                def style_function(feature):
                    val = feature["properties"].get(fld)
                    if val is None:
                        return {
                            "fillColor": "#cccccc",
                            "color": "#666666",
                            "weight": 1,
                            "fillOpacity": 0.3,
                        }
                    clamped = max(lo, min(hi, val))
                    return {
                        "fillColor": cmap(clamped),
                        "color": "#333333",
                        "weight": 1,
                        "fillOpacity": 0.7,
                    }
                return style_function

            def highlight_function(feature):
                return {"weight": 3, "color": "#000000", "fillOpacity": 0.85}

            fg = folium.FeatureGroup(name=layer_def["name"], show=layer_def["show"])

            geojson_layer = folium.GeoJson(
                geojson_data,
                style_function=make_style_fn(colormap, field, vmin, vmax),
                highlight_function=highlight_function,
                tooltip=folium.GeoJsonTooltip(
                    fields=["city_name_geo", "deviation_pct", "tx_median_man", "op_median_man", "tx_count", "op_count"],
                    aliases=["市区町村", "乖離率(%)", "取引中央値(万円/㎡)", "公示中央値(万円/㎡)", "取引件数", "公示地点数"],
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
            )
            geojson_layer.add_to(fg)
            fg.add_to(m)

            groups["指標切替"].append(fg)

        GroupedLayerControl(groups, exclusive_groups=True, collapsed=False).add_to(m)

        # レイヤー切替時にカラーバーも切り替えるJavaScript
        name_to_id = {l["name"]: f"legend-{i}" for i, l in enumerate(layers)}
        mapping_js = json.dumps(name_to_id, ensure_ascii=False)
        toggle_script = f"""
        <script>
        document.addEventListener('DOMContentLoaded', function() {{
            var mapping = {mapping_js};
            var allIds = Object.values(mapping);

            function setupListeners() {{
                var container = document.querySelector('.leaflet-control-layers');
                if (!container) return false;
                container.addEventListener('click', function() {{
                    setTimeout(function() {{
                        var labels = container.querySelectorAll('label');
                        labels.forEach(function(label) {{
                            var radio = label.querySelector('input[type="radio"]');
                            if (radio && radio.checked) {{
                                var name = label.textContent.trim();
                                allIds.forEach(function(id) {{
                                    document.getElementById(id).style.display = 'none';
                                }});
                                if (mapping[name]) {{
                                    document.getElementById(mapping[name]).style.display = 'block';
                                }}
                            }}
                        }});
                    }}, 50);
                }});
                return true;
            }}

            var attempts = 0;
            var interval = setInterval(function() {{
                if (setupListeners() || attempts > 50) clearInterval(interval);
                attempts++;
            }}, 200);
        }});
        </script>
        """
        m.get_root().html.add_child(folium.Element(toggle_script))

        title_html = """
        <div style="position: fixed; top: 10px; left: 50px; z-index: 1000;
                    background: white; padding: 12px 20px; border-radius: 5px;
                    border: 2px solid #333; box-shadow: 3px 3px 6px rgba(0,0,0,0.3);
                    max-width: 420px; line-height: 1.6;">
            <div style="font-size: 16px; font-weight: bold; margin-bottom: 6px;">
                不動産歪みマップ: 取引価格 vs 公示価格
            </div>
            <div style="font-size: 12px; color: #444;">
                <span style="white-space: nowrap;">乖離率 = (取引㎡単価中央値 − 公示価格中央値) / 公示価格中央値 × 100%</span><br>
                対象: 宅地(土地のみ)取引 / 住宅地の公示価格<br>
                期間: 2022〜2025年 ｜ 取引件数10以下の自治体は除外
            </div>
        </div>
        """
        m.get_root().html.add_child(folium.Element(title_html))

        credit_html = """
        <div style="position: fixed; bottom: 20px; right: 10px; z-index: 1000;
                    background: rgba(255,255,255,0.92); padding: 8px 12px; border-radius: 4px;
                    border: 1px solid #999; max-width: 480px; line-height: 1.5;">
            <div style="font-size: 11px; color: #333;">
                出典：国土交通省 不動産情報ライブラリ（加工して作成）<br>
                <span style="font-size: 10px; color: #666;">
                このサービスは、国土交通省の不動産情報ライブラリのAPI機能を使用していますが、
                提供情報の最新性、正確性、完全性等が保証されたものではありません
                </span>
            </div>
        </div>
        """
        m.get_root().html.add_child(folium.Element(credit_html))

        return m
