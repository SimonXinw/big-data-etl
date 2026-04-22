/**
 * 可选 Chart.js 面板：按 table_catalog 的 chart / chartConfig 渲染。
 */

const chartTypeMap = {
  line: "line",
  bar: "bar",
};

const destroyChart = (chartRef) => {
  if (chartRef.current) {
    chartRef.current.destroy();
    chartRef.current = null;
  }
};

const pickNumeric = (value) => {
  if (value === null || value === undefined) {
    return null;
  }

  const parsed = Number(value);

  if (Number.isFinite(parsed)) {
    return parsed;
  }

  return null;
};

export const renderChartPanel = async ({
  chartCanvas,
  chartCard,
  chartRef,
  rows,
  tableMeta,
}) => {
  destroyChart(chartRef);

  if (!chartCanvas || !chartCard) {
    return;
  }

  const chartKind = tableMeta?.chart ?? "none";

  if (chartKind === "none" || !rows?.length) {
    chartCard.hidden = true;

    return;
  }

  const labelColumn = tableMeta?.chartConfig?.labelColumn;
  const valueColumn = tableMeta?.chartConfig?.valueColumn;

  if (!labelColumn || !valueColumn) {
    chartCard.hidden = true;

    return;
  }

  const labels = [];
  const values = [];

  for (const row of rows) {
    const labelRaw = row[labelColumn];
    const value = pickNumeric(row[valueColumn]);

    if (value === null) {
      continue;
    }

    labels.push(labelRaw === null || labelRaw === undefined ? "" : String(labelRaw));
    values.push(value);
  }

  if (!labels.length) {
    chartCard.hidden = true;

    return;
  }

  chartCard.hidden = false;

  const chartType = chartTypeMap[chartKind] ?? "bar";

  const { Chart, registerables } = await import("https://cdn.jsdelivr.net/npm/chart.js@4.4.6/+esm");

  Chart.register(...registerables);

  const datasetColor = getComputedStyle(document.documentElement).getPropertyValue("--accent").trim()
    || "#5b9cff";

  chartRef.current = new Chart(chartCanvas, {
    type: chartType,
    data: {
      labels,
      datasets: [
        {
          label: valueColumn,
          data: values,
          borderColor: datasetColor,
          backgroundColor: chartType === "bar" ? withAlpha(datasetColor, 0.35) : withAlpha(datasetColor, 0.2),
          tension: 0.25,
          fill: chartType === "line",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: getMutedColor() },
        },
      },
      scales: {
        x: {
          ticks: { color: getMutedColor(), maxRotation: 45, minRotation: 0 },
          grid: { color: gridColor() },
        },
        y: {
          ticks: { color: getMutedColor() },
          grid: { color: gridColor() },
        },
      },
    },
  });
};

const getMutedColor = () => getComputedStyle(document.documentElement).getPropertyValue("--muted").trim()
  || "#9aa7b8";

const gridColor = () => "rgba(128, 140, 160, 0.2)";

const withAlpha = (hex, alpha) => {
  const match = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);

  if (!match) {
    return `rgba(91, 156, 255, ${alpha})`;
  }

  const r = Number.parseInt(match[1], 16);
  const g = Number.parseInt(match[2], 16);
  const b = Number.parseInt(match[3], 16);

  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};
