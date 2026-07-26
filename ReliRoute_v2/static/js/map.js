/* ==========================================================================
   ReliRoute 共用地圖模組 (map.js)
   負責:初始化 Leaflet 地圖、載入 OSM 底圖、risk_level 顏色對應、
        清除舊圖層、載入 GeoJSON 並綁定 Popup。
   ========================================================================== */

const RISK_COLORS = {
  low: "#22C55E",
  medium: "#F59E0B",
  high: "#EF4444",
  // reliability_level(穩定度分析頁使用的真實資料欄位)對應的顏色
  "穩定": "#22C55E",
  "普通": "#F59E0B",
  "不穩定": "#F97316",
  "高風險": "#EF4444"
};

const TAIWAN_CENTER = [23.9, 121.0];

function riskColor(riskLevel) {
  return RISK_COLORS[riskLevel] || "#94A3B8";
}

function initMap(elementId, center, zoom) {
  const map = L.map(elementId, { zoomControl: true }).setView(
    center || TAIWAN_CENTER,
    zoom || 8
  );

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
    maxZoom: 18
  }).addTo(map);

  return map;
}

function clearLayer(layerGroup) {
  if (layerGroup) {
    layerGroup.clearLayers();
  }
}

function loadGeoJSON(map, geojson, options) {
  const opts = options || {};
  const weight = opts.weight || 6;

  const layer = L.geoJSON(geojson, {
    style: (feature) => ({
      color: riskColor(feature.properties.risk_level || feature.properties.reliability_level),
      weight: weight,
      opacity: 0.85,
      lineCap: "round"
    }),
    onEachFeature: (feature, featureLayer) => {
      if (opts.popupHtml) {
        featureLayer.bindPopup(opts.popupHtml(feature.properties));
      }
      if (opts.onClick) {
        featureLayer.on("click", () => opts.onClick(feature.properties, featureLayer));
      }
      featureLayer.on("mouseover", function () {
        this.setStyle({ weight: weight + 3 });
      });
      featureLayer.on("mouseout", function () {
        this.setStyle({ weight: weight });
      });
    }
  });

  layer.addTo(map);

  if (opts.fitBounds !== false && geojson.features && geojson.features.length > 0) {
    map.fitBounds(layer.getBounds(), { padding: [30, 30] });
  }

  return layer;
}

function addGantryMarker(map, gantry, options) {
  const opts = options || {};
  const icon = L.divIcon({
    className: "gantry-marker",
    html: `<div style="background:${opts.color || "#2563EB"};width:14px;height:14px;border-radius:50%;border:3px solid #fff;box-shadow:0 0 0 2px ${opts.color || "#2563EB"};"></div>`,
    iconSize: [14, 14]
  });
  const marker = L.marker([gantry.latitude, gantry.longitude], { icon }).addTo(map);
  if (opts.label) {
    marker.bindTooltip(opts.label, { permanent: false, direction: "top" });
  }
  return marker;
}
