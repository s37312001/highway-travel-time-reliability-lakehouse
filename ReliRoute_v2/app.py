# app.py
# 建立 Flask 應用程式,註冊頁面路由與 API 的 Blueprint
#
# v2 是標準 Flask 專案結構:前端頁面(templates/static)與 API
# 都由同一個 Flask App 提供,所以不需要再處理跨來源(CORS)問題。

from flask import Flask, render_template

from routes.api import api_bp

# 建立 Flask 應用程式
app = Flask(__name__)

# 註冊 API Blueprint,所有 API 都會以 /api 開頭
app.register_blueprint(api_bp, url_prefix="/api")


# 首頁
@app.route("/")
def index():
    return render_template("index.html")


# 穩定度分析頁
@app.route("/analysis")
def analysis():
    return render_template("analysis.html")


# 旅行時間查詢頁
@app.route("/travel")
def travel():
    return render_template("travel.html")


# 關於平台頁
@app.route("/about")
def about():
    return render_template("about.html")


if __name__ == "__main__":
    # 啟動開發伺服器
    app.run(debug=True)
