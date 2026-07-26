/* ==========================================================================
   ReliRoute 共用邏輯 (app.js)
   目前負責:依目前頁面檔名,高亮 Sidebar 對應選單項目。
   ========================================================================== */

function highlightActiveNav() {
  const currentPage = window.location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll(".sidebar-nav .nav-link").forEach((link) => {
    const href = link.getAttribute("href");
    if (href === currentPage) {
      link.classList.add("active");
    } else {
      link.classList.remove("active");
    }
  });
}

document.addEventListener("DOMContentLoaded", highlightActiveNav);
