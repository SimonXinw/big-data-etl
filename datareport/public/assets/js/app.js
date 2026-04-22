/**
 * 数据报表前端入口（原生 ES Module，无打包步骤）。
 */

import { fetchCatalog, fetchClientConfig, fetchDataset, fetchHealth } from "./api.js";
import { renderTable } from "./renderTable.js";
import { renderChartPanel } from "./chartPanel.js";
import { renderMetrics } from "./metrics.js";

const layerSelect = document.querySelector("#layerSelect");
const tableSelect = document.querySelector("#tableSelect");
const limitInput = document.querySelector("#limitInput");
const refreshButton = document.querySelector("#refreshButton");
const resetButton = document.querySelector("#resetButton");
const tableMount = document.querySelector("#tableMount");
const chartCanvas = document.querySelector("#chartCanvas");
const chartCard = document.querySelector("#chartCard");
const layoutSplit = document.querySelector("#layoutSplit");
const statusLine = document.querySelector("#statusLine");
const toast = document.querySelector("#toast");
const metricsMount = document.querySelector("#metricsMount");
const dataPathBadge = document.querySelector("#dataPathBadge");

let catalog = null;
let clientConfig = null;
let currentLayerId = "";
const chartRef = { current: null };

const showToast = (message, isError) => {
  if (!toast) {
    return;
  }

  toast.textContent = message;
  toast.classList.toggle("error", Boolean(isError));
  toast.classList.add("visible");

  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => {
    toast.classList.remove("visible");
  }, 5200);
};

const findLayer = (layerId) => catalog?.layers?.find((layer) => layer.id === layerId) ?? null;

const findTableMeta = (layerId, tableName) => {
  const layer = findLayer(layerId);

  if (!layer) {
    return null;
  }

  return layer.tables.find((table) => table.name === tableName) ?? null;
};

const populateLayerOptions = () => {
  if (!layerSelect || !catalog?.layers) {
    return;
  }

  layerSelect.innerHTML = "";

  for (const layer of catalog.layers) {
    const option = document.createElement("option");

    option.value = layer.id;
    option.textContent = layer.label;
    layerSelect.appendChild(option);
  }
};

const populateTableOptions = (layerId) => {
  if (!tableSelect) {
    return;
  }

  tableSelect.innerHTML = "";

  const layer = findLayer(layerId);

  if (!layer) {
    return;
  }

  for (const table of layer.tables) {
    const option = document.createElement("option");

    option.value = table.name;
    option.textContent = table.label;
    tableSelect.appendChild(option);
  }
};

const syncLayoutForChart = async (tableMeta, rows) => {
  if (!layoutSplit || !chartCard) {
    return;
  }

  const shouldSplit = tableMeta?.chart && tableMeta.chart !== "none" && rows?.length;

  layoutSplit.classList.toggle("has_chart", Boolean(shouldSplit));

  await renderChartPanel({
    chartCanvas,
    chartCard,
    chartRef,
    rows,
    tableMeta,
  });
};

const loadRows = async () => {
  if (!layerSelect || !tableSelect) {
    return;
  }

  const layerId = layerSelect.value;
  const tableName = tableSelect.value;
  const limit = Number(limitInput?.value ?? 500);
  const tableMeta = findTableMeta(layerId, tableName);
  const layer = findLayer(layerId);

  if (!layer || !tableMeta) {
    showToast("请选择有效的分层与表。", true);

    return;
  }

  statusLine.innerHTML = "正在查询…";

  try {
    const payload = await fetchDataset(clientConfig, tableMeta.schema, tableName, limit);

    renderMetrics(metricsMount, payload.rows, payload.columns, tableMeta);
    renderTable(tableMount, payload.columns, payload.rows);
    await syncLayoutForChart(tableMeta, payload.rows);

    const sourceLabel = payload.source === "postgres_direct"
      ? "Postgres 直连"
      : payload.source === "supabase_rest_browser"
        ? "Supabase REST（浏览器）"
        : "Supabase REST（同源转发）";

    statusLine.innerHTML = `<strong>已加载</strong> ${payload.row_count} 行 · <code>${payload.schema}.${payload.table}</code> · ${sourceLabel}`;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    showToast(message, true);
    statusLine.innerHTML = `<strong>加载失败</strong> · ${message}`;
    renderMetrics(metricsMount, [], [], tableMeta);
    renderTable(tableMount, [], []);
    await syncLayoutForChart(tableMeta, []);
  }
};

const wireEvents = () => {
  layerSelect?.addEventListener("change", () => {
    currentLayerId = layerSelect.value;
    populateTableOptions(currentLayerId);
    void loadRows();
  });

  tableSelect?.addEventListener("change", () => {
    void loadRows();
  });

  refreshButton?.addEventListener("click", () => {
    void loadRows();
  });

  resetButton?.addEventListener("click", () => {
    if (limitInput) {
      limitInput.value = "500";
    }

    void loadRows();
  });
};

const init = async () => {
  wireEvents();

  try {
    clientConfig = await fetchClientConfig();
  } catch {
    clientConfig = null;
  }

  try {
    const health = await fetchHealth();

    if (dataPathBadge && health?.default_data_path) {
      const map = {
        supabase_rest: "数据：Supabase REST",
        postgres_direct: "数据：Postgres 直连",
        none: "数据：未配置",
      };

      dataPathBadge.textContent = map[health.default_data_path] ?? `数据：${health.default_data_path}`;
    }

    if (!health?.supabase_configured && !health?.postgres_configured) {
      showToast("未检测到 Supabase 或 Postgres 配置：请在 .env 中配置 SUPABASE_* 或 ETL_POSTGRES_DSN。", true);
    }
  } catch {
    showToast("健康检查失败：请确认已通过 datareport/serve.py 启动服务。", true);
  }

  try {
    catalog = await fetchCatalog();
    currentLayerId = catalog.layers[0]?.id ?? "";
    populateLayerOptions();

    if (layerSelect && currentLayerId) {
      layerSelect.value = currentLayerId;
    }

    populateTableOptions(currentLayerId);
    await loadRows();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    showToast(`目录加载失败：${message}`, true);
  }
};

void init();
