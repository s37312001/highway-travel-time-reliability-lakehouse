/* ==========================================================================
   ReliRoute 旅行時間查詢頁 (travel.js)
   ========================================================================== */

let travelMap = null;
let travelLayer = null;
let gantryListCache = [];

// 呼叫 GET /api/gantries(網址集中定義在 api.js 的 API.gantries),取得起訖點清單
function fetchGantryList() {
  return fetch(API.gantries).then((res) => res.json());
}

// 把 API 回傳的資料填入起點/終點下拉選單
// option 顯示 name,option value 使用 gantry_id
function populateGantrySelects(gantries) {
  const startSelect = document.getElementById("startSelect");
  const endSelect = document.getElementById("endSelect");

  gantries.forEach((g) => {
    startSelect.add(new Option(g.name, g.gantry_id));
    endSelect.add(new Option(g.name, g.gantry_id));
  });

  if (gantries.length > 1) {
    startSelect.value = gantries[0].gantry_id;
    endSelect.value = gantries[gantries.length - 1].gantry_id;
  }
}

function updateModeVisibility() {
  const mode = document.getElementById("modeSelect").value;
  document.getElementById("dateGroup").classList.toggle("d-none", mode !== "date");
  document.getElementById("weekdayGroup").classList.toggle("d-none", mode !== "weekday");
  document.getElementById("holidayGroup").classList.toggle("d-none", mode !== "holiday");
}

  //收集資料
function buildPayload() {
  const mode = document.getElementById("modeSelect").value;
  const payload = {
    start: document.getElementById("startSelect").value,
    end: document.getElementById("endSelect").value,
    mode: mode,
    time: document.getElementById("timeInput").value
  };

  if (mode === "date") {
    payload.date = document.getElementById("dateInput").value;
  } else if (mode === "weekday") {
    payload.weekday = document.getElementById("weekdaySelect").value;
  } else if (mode === "holiday") {
    payload.holiday = document.getElementById("holidaySelect").value;
    payload.range = document.getElementById("rangeSelect").value;
  }

  return payload;
}

// 路段顏色:只用來區分相鄰的 segment,不代表壅塞、風險或穩定度
// 依 ReliRoute 科技藍視覺,固定用這組藍、青、紫藍、靛色階輪流分配
const SEGMENT_COLORS = ["#2563EB", "#38BDF8", "#6366F1", "#7C3AED"];

function travelPopupHtml(segment) {
  return `<div class="popup-row">旅行時間:<strong>${segment.travel_time_sec} 秒</strong></div>`;
}

// 依 segment 陣列,逐段建立 Leaflet Polyline,放進同一個 LayerGroup 管理
// 注意:Leaflet 座標順序是 [latitude, longitude],跟 GeoJSON 的 [lon, lat] 相反
function buildSegmentLayerGroup(segments) {
  const layerGroup = L.layerGroup();

  segments.forEach((segment, index) => {
    const color = SEGMENT_COLORS[index % SEGMENT_COLORS.length];
    const latlngs = [
      [segment.start_latitude, segment.start_longitude],
      [segment.end_latitude, segment.end_longitude]
    ];

    const polyline = L.polyline(latlngs, {
      color: color,
      weight: 7,
      opacity: 0.85,
      lineCap: "round"
    });

    polyline.bindPopup(travelPopupHtml(segment));
    layerGroup.addLayer(polyline);
  });

  return layerGroup;
}

// 在右側資訊卡顯示訊息(共用:錯誤訊息、提示訊息都用這個)
function showInfoCardMessage(message, iconClass) {
  if (travelLayer) {
    travelMap.removeLayer(travelLayer);
    travelLayer = null;
  }

  document.getElementById("totalTimeContent").classList.add("d-none");

  const placeholder = document.getElementById("totalTimePlaceholder");
  placeholder.classList.remove("d-none");
  placeholder.innerHTML = `<i class="bi ${iconClass} d-block mb-2" style="font-size: 1.6rem;"></i>${message}`;
}

function showNoDataMessage(message) {
  showInfoCardMessage(message, "bi-exclamation-circle");
}

// 檢查查詢條件是否合法,合法回傳 null,不合法回傳錯誤訊息文字
function validatePayload(payload) {
  if (!payload.start) return "起點不可為空";
  if (!payload.end) return "終點不可為空";
  if (payload.start === payload.end) return "起點與終點不可相同";
  if (!payload.time) return "時間不可為空";

  if (payload.mode === "date" && !payload.date) return "日期不可為空";
  if (payload.mode === "weekday" && !payload.weekday) return "星期不可為空";
  if (payload.mode === "holiday" && (!payload.holiday || !payload.range)) {
    return "連假名稱與連假區間不可為空";
  }

  return null;
}

function updateTotalTimePanel(totalMinutes) {
  document.getElementById("totalTimePlaceholder").classList.add("d-none");
  document.getElementById("totalTimeContent").classList.remove("d-none");
  document.getElementById("totalTimeValue").textContent = totalMinutes;
}

function performSearch() {
  // 每次新查詢前,先清掉上一輪畫的路段 Layer
  if (travelLayer) {
    travelMap.removeLayer(travelLayer);
    travelLayer = null;
  }

  const payload = buildPayload();
  const errorMessage = validatePayload(payload);

  if (errorMessage) {
    showNoDataMessage(errorMessage);
    return;
  }

  // 呼叫 travel-time API,payload 已包含起訖點與查詢條件
  fetchTravelTime(payload)
    .then((result) => {
      // 處理 API 回傳
      const segments = (result && result.segments) || [];

      if (segments.length === 0) {
        showNoDataMessage("查無符合條件的旅行時間資料");
        return;
      }

      // 繪製分段路線
      travelLayer = buildSegmentLayerGroup(segments);
      travelLayer.addTo(travelMap);

      updateTotalTimePanel(result.total_travel_time_min);
    })
    .catch((error) => {
      console.error(error);
      showNoDataMessage("旅行時間查詢失敗,請稍後再試");
    });
}

document.addEventListener("DOMContentLoaded", () => {
  travelMap = initMap("travelMap", [23.9, 121.0], 8);

  const now = new Date();
  document.getElementById("dateInput").value = now.toISOString().slice(0, 10);
  document.getElementById("timeInput").value = now.toTimeString().slice(0, 5);

  fetchGantryList().then((list) => {
    gantryListCache = list;
    populateGantrySelects(list);
  });

  // 部分瀏覽器重新整理時會自動還原下拉選單先前的值(且不觸發 change 事件),
  // 因此明確重置為預設模式,確保欄位顯示與選單文字一致。
  document.getElementById("modeSelect").value = "date";
  updateModeVisibility();

  document.getElementById("modeSelect").addEventListener("change", updateModeVisibility);
  document.getElementById("searchBtn").addEventListener("click", performSearch);
});
