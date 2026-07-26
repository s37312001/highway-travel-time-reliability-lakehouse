/* ==========================================================================
   ReliRoute 穩定度分析頁 (analysis.js)
   ========================================================================== */

// reliability_level 對應的顯示顏色(與 map.js 的 RISK_COLORS 一致)
const RELIABILITY_BADGE_COLORS = {
  "穩定": { bg: "rgba(34, 197, 94, 0.12)", color: "#22C55E" },
  "普通": { bg: "rgba(245, 158, 11, 0.12)", color: "#F59E0B" },
  "不穩定": { bg: "rgba(249, 115, 22, 0.12)", color: "#F97316" },
  "高風險": { bg: "rgba(239, 68, 68, 0.12)", color: "#EF4444" }
};

let analysisMap = null;
let reliabilityLayer = null;

// summary_level 決定 time_period / day_type 是否需要讓使用者選擇
function updateFieldVisibility() {
  const summaryLevel = document.getElementById("summaryLevelSelect").value;
  const timePeriodGroup = document.getElementById("timePeriodGroup");
  const dayTypeGroup = document.getElementById("dayTypeGroup");

  const needsTimePeriod = summaryLevel === "L2_TIME_PERIOD" || summaryLevel === "L3_DAY_TYPE_TIME_PERIOD";
  const needsDayType = summaryLevel === "L1_DAY_TYPE" || summaryLevel === "L3_DAY_TYPE_TIME_PERIOD";

  timePeriodGroup.classList.toggle("d-none", !needsTimePeriod);
  dayTypeGroup.classList.toggle("d-none", !needsDayType);
}

// 組成呼叫 /api/reliability 需要的 query parameters
// summary_level 用不到 time_period / day_type 時一律送 ALL
function getCurrentFilters() {
  const summaryLevel = document.getElementById("summaryLevelSelect").value;
  const needsTimePeriod = summaryLevel === "L2_TIME_PERIOD" || summaryLevel === "L3_DAY_TYPE_TIME_PERIOD";
  const needsDayType = summaryLevel === "L1_DAY_TYPE" || summaryLevel === "L3_DAY_TYPE_TIME_PERIOD";

  return {
    summary_level: summaryLevel,
    day_type: needsDayType ? document.getElementById("dayTypeSelect").value : "ALL",
    time_period: needsTimePeriod ? document.getElementById("timePeriodSelect").value : "ALL",
    direction: document.getElementById("directionSelect").value
  };
}

// 呼叫 GET /api/reliability(網址集中定義在 api.js 的 API.reliability)
function fetchReliabilityFromApi(params) {
  const query = new URLSearchParams(params).toString();
  return fetch(`${API.reliability}?${query}`).then((res) => res.json());
}

function reliabilityPopupHtml(props) {
  return `
    <div class="popup-title">${props.pair_id}</div>
    <div class="popup-row">路段:<strong>${props.route_name}</strong></div>
    <div class="popup-row">中位數:<strong>${props.p50_travel_time_sec} 秒</strong></div>
    <div class="popup-row">P95:<strong>${props.p95_travel_time_sec} 秒</strong></div>
    <div class="popup-row">緩衝時間:<strong>${props.buffer_time_sec} 秒</strong></div>
    <div class="popup-row">穩定度:<strong>${props.reliability_level}</strong></div>
  `;
}

// 把秒數格式化成「X 分 X 秒」,無效值(null/undefined/空字串/非數字)顯示 —
function formatDuration(seconds) {
  if (typeof seconds !== "number" || Number.isNaN(seconds)) {
    return "—";
  }

  const totalSeconds = Math.round(seconds);
  const minutes = Math.floor(totalSeconds / 60);
  const remainingSeconds = totalSeconds % 60;

  if (minutes === 0) {
    return `${remainingSeconds} 秒`;
  }
  if (remainingSeconds === 0) {
    return `${minutes} 分`;
  }
  return `${minutes} 分 ${remainingSeconds} 秒`;
}

function updateRouteInfoPanel(props) {
  document.getElementById("routeInfoPlaceholder").classList.add("d-none");
  document.getElementById("routeInfoContent").classList.remove("d-none");

  document.getElementById("infoPairId").textContent = props.pair_id;
  document.getElementById("infoRouteName").textContent = props.route_name;
  document.getElementById("infoMedian").textContent = formatDuration(props.p50_travel_time_sec);
  document.getElementById("infoP95").textContent = formatDuration(props.p95_travel_time_sec);
  document.getElementById("infoBuffer").textContent = formatDuration(props.buffer_time_sec);

  const badge = document.getElementById("infoRiskBadge");
  const colors = RELIABILITY_BADGE_COLORS[props.reliability_level] || { bg: "rgba(148, 163, 184, 0.12)", color: "#94A3B8" };
  badge.textContent = props.reliability_level;
  badge.className = "badge-risk";
  badge.style.backgroundColor = colors.bg;
  badge.style.color = colors.color;
}

function loadReliabilityLayer() {
  const filters = getCurrentFilters();

  fetchReliabilityFromApi(filters).then((geojson) => {
    if (reliabilityLayer) {
      analysisMap.removeLayer(reliabilityLayer);
      reliabilityLayer = null;
    }

    reliabilityLayer = loadGeoJSON(analysisMap, geojson, {
      weight: 6,
      popupHtml: reliabilityPopupHtml,
      onClick: updateRouteInfoPanel
    });
  });
}

// 地圖右下角的穩定度等級圖例,顏色直接引用 map.js 的 RISK_COLORS,
// 確保跟路段 Polyline 顏色完全一致(不要另外定義一套顏色)
function addReliabilityLegend(map) {
  const legend = L.control({ position: "bottomright" });

  legend.onAdd = function () {
    const div = L.DomUtil.create("div", "reliability-legend");
    const levels = ["穩定", "普通", "不穩定", "高風險"];

    const rows = levels
      .map(
        (level) => `
          <div class="reliability-legend-row">
            <span class="reliability-legend-dot" style="background-color: ${RISK_COLORS[level]};"></span>
            <span>${level}</span>
          </div>
        `
      )
      .join("");

    div.innerHTML = `<div class="reliability-legend-title">穩定度等級</div>${rows}`;
    return div;
  };

  legend.addTo(map);
}

document.addEventListener("DOMContentLoaded", () => {
  analysisMap = initMap("analysisMap", [23.9, 121.0], 8);
  addReliabilityLegend(analysisMap);

  updateFieldVisibility();
  loadReliabilityLayer();

  document.getElementById("summaryLevelSelect").addEventListener("change", updateFieldVisibility);
  document.getElementById("updateMapBtn").addEventListener("click", loadReliabilityLayer);
});
