# routes/api.py
# 建立 API 的 Blueprint,並把每個 Resource 掛到對應的路徑

from flask import Blueprint
from flask_restful import Api

from resources.reliability_api import ReliabilityResource
from resources.gantry_api import GantryResource
from resources.travel_time_api import TravelTimeResource

# 建立名稱為 api 的 Blueprint
api_bp = Blueprint("api", __name__)

# 讓 Flask-RESTful 管理這個 Blueprint 底下的路由
api = Api(api_bp)

# 路徑對應:每個 Resource 負責一支 API
api.add_resource(ReliabilityResource, "/reliability")
api.add_resource(GantryResource, "/gantries")
api.add_resource(TravelTimeResource, "/travel-time")
