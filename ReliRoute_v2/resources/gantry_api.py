# gantry_api.py
# 龍門架清單 API 的 Resource(對應 GET /api/gantries)
# 這支 API 只負責讀取 data/gantry.csv,轉換成 JSON 陣列後回傳

import os
import csv

from flask_restful import Resource

# gantry.csv 檔案的路徑(位於 backend/data/ 底下)
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "gantry.csv")


class GantryResource(Resource):
    # 處理 GET 請求
    def get(self):
        # 檔案不存在時,回傳 404 與錯誤訊息
        if not os.path.exists(DATA_PATH):
            return {"error": "gantry.csv not found"}, 404

        # 讀取 CSV,每一行格式為:名稱,gantry_id
        gantries = []
        with open(DATA_PATH, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                name, gantry_id = row[0], row[1]
                gantries.append({"name": name, "gantry_id": gantry_id})

        return gantries
