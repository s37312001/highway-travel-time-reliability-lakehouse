/* ==========================================================================
   ReliRoute API 層 (api.js)

   集中定義三支 API 的路徑(同源相對路徑,不寫死主機網址)。
   reliability / gantries 的實際呼叫寫在 analysis.js / travel.js,
   兩邊都直接使用這裡的 API.reliability / API.gantries。
   ========================================================================== */

const API = {
  reliability: "/api/reliability",
  gantries: "/api/gantries",
  travelTime: "/api/travel-time"
};

/* -------------------------------------------------------------------------- */
/* POST /api/travel-time - 真正呼叫後端 API                                   */
/* -------------------------------------------------------------------------- */

// 呼叫 travel-time API:傳送查詢條件(payload),取回旅行時間結果
function fetchTravelTime(payload) {
  return fetch(API.travelTime, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  }).then((res) => {
    if (!res.ok) {
      throw new Error(`POST ${API.travelTime} 回傳狀態碼 ${res.status}`);
    }
    return res.json();
  });
}
