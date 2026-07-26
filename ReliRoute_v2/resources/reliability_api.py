# reliability_api.py
# 穩定度分析 API 的 Resource(對應 GET /api/reliability)
# 讀取 data/reliability_map.geojson,依 query parameters 篩選後回傳 GeoJSON
# 只做欄位篩選與挑選,不做任何統計運算

import os
import csv
import json

from flask import request
from flask_restful import Resource

# reliability_map.geojson 檔案的路徑(位於 backend/data/ 底下)
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "reliability_map.geojson")

# location.csv 檔案的路徑(pair_id -> 中文路段名稱 pair_roadname,只給 analysis 頁用)
LOCATION_DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "location.csv")

# 可以用來篩選 features 的 query parameters
FILTER_KEYS = ["summary_level", "day_type", "time_period", "direction"]

# 回傳給前端時,只保留這些欄位(route_name 另外處理,不在這裡面)
KEEP_PROPERTIES = [
    "pair_id",
    "p50_travel_time_sec",
    "p95_travel_time_sec",
    "buffer_time_sec",
    "reliability_level",
    "summary_level",
    "day_type",
    "time_period",
    "direction",
]


def _load_geojson():
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


# 讀取 location.csv,建立 pair_id -> pair_roadname(中文路段名稱)的對照表
# 檔案不存在時回傳空表,讓後面的查詢自動 fallback,不會讓 API 出錯
def _load_location_map():
    location_map = {}
    if not os.path.exists(LOCATION_DATA_PATH):
        return location_map

    with open(LOCATION_DATA_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            location_map[row["pair_id"]] = row["pair_roadname"]

    return location_map


# 檢查一個 feature 的 properties 是否符合所有篩選條件
def _matches_filters(properties, filters):
    for key, value in filters.items():
        if properties.get(key) != value:
            return False
    return True


# 只挑出需要回傳給前端的欄位,並依 pair_id 加上中文路段名稱 route_name
# location.csv 找不到對應 pair_id 時,route_name 用 pair_id 本身當 fallback
def _pick_properties(properties, location_map):
    picked = {key: properties.get(key) for key in KEEP_PROPERTIES}
    pair_id = properties.get("pair_id")
    picked["route_name"] = location_map.get(pair_id, pair_id)
    return picked


class ReliabilityResource(Resource):
    # 處理 GET 請求
    def get(self):
        # 檔案不存在時,回傳 404 與錯誤訊息
        if not os.path.exists(DATA_PATH):
            return {"error": "reliability_map.geojson not found"}, 404

        geojson_data = _load_geojson()
        location_map = _load_location_map()

        # 只用有帶值的 query parameter 當篩選條件
        filters = {
            key: request.args.get(key)
            for key in FILTER_KEYS
            if request.args.get(key)
        }

        features = []
        for feature in geojson_data.get("features", []):
            properties = feature.get("properties", {})
            if not _matches_filters(properties, filters):
                continue

            features.append({
                "type": "Feature",
                "properties": _pick_properties(properties, location_map),
                "geometry": feature.get("geometry"),
            })

        # 沒有符合的資料時,回傳空的 FeatureCollection
        return {"type": "FeatureCollection", "features": features}
