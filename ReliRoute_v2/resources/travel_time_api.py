# travel_time_api.py
# 旅行時間查詢 API 的 Resource(對應 POST /api/travel-time)
# 目前只有基本架構,還沒有真正的資料邏輯

from flask_restful import Resource


class TravelTimeResource(Resource):
    # 處理 POST 請求
    def post(self):
        # 之後會依據起訖點、時間等條件回傳旅行時間結果
        return {"message": "travel time api not implemented yet"}
