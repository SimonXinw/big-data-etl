/**
 * 从当前页数据行推导 KPI 卡片（优先读 table_catalog 的 kpis，否则启发式）。
 */

const sumColumn = (rows, column) => {
  let total = 0;
  let count = 0;

  for (const row of rows) {
    const raw = row[column];
    const value = typeof raw === "number" ? raw : Number(raw);

    if (!Number.isFinite(value)) {
      continue;
    }

    total += value;
    count += 1;
  }

  return { total, count };
};

const formatNumber = (value) => {
  if (!Number.isFinite(value)) {
    return "—";
  }

  const abs = Math.abs(value);

  if (abs >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  }

  if (abs >= 10_000) {
    return `${(value / 1000).toFixed(1)}k`;
  }

  if (Number.isInteger(value)) {
    return String(value);
  }

  return value.toFixed(2);
};

const heuristicNumericColumns = (columns, rows) => {
  const skip = new Set([
    "customer_id",
    "order_id",
    "legacy_order_id",
    "summary_key",
    "shopify_gid",
    "marketing_intention",
    "product_quantity",
  ]);

  const scored = [];

  for (const column of columns) {
    if (skip.has(column) || /_id$/i.test(column)) {
      continue;
    }

    const { total, count } = sumColumn(rows, column);

    if (count === 0) {
      continue;
    }

    scored.push({ column, total, count });
  }

  scored.sort((a, b) => Math.abs(b.total) - Math.abs(a.total));

  return scored.slice(0, 4);
};

const buildKpiCards = (rows, columns, tableMeta) => {
  const cards = [];

  cards.push({
    label: "返回行数",
    value: formatNumber(rows.length),
    hint: "当前请求范围内的行数（受 Limit 影响）",
    tone: "neutral",
  });

  const declared = Array.isArray(tableMeta?.kpis) ? tableMeta.kpis : [];

  for (const item of declared) {
    const column = item.column;
    const agg = item.agg === "avg" ? "avg" : "sum";

    if (!column || !columns.includes(column)) {
      continue;
    }

    if (agg === "avg") {
      const { total, count } = sumColumn(rows, column);
      const avg = count ? total / count : NaN;

      cards.push({
        label: item.label || column,
        value: formatNumber(avg),
        hint: item.hint || `列 ${column} 的平均值`,
        tone: item.tone || "accent",
      });

      continue;
    }

    const { total, count } = sumColumn(rows, column);

    if (!count) {
      continue;
    }

    cards.push({
      label: item.label || column,
      value: formatNumber(total),
      hint: item.hint || `列 ${column} 求和`,
      tone: item.tone || "accent",
    });
  }

  if (declared.length) {
    return cards;
  }

  const picks = heuristicNumericColumns(columns, rows);

  for (const pick of picks) {
    cards.push({
      label: `∑ ${pick.column}`,
      value: formatNumber(pick.total),
      hint: "启发式数值列求和",
      tone: "soft",
    });
  }

  return cards;
};

export const renderMetrics = (mount, rows, columns, tableMeta) => {
  if (!mount) {
    return;
  }

  mount.innerHTML = "";

  const cards = buildKpiCards(rows, columns, tableMeta);

  for (const card of cards) {
    const node = document.createElement("article");

    node.className = `kpi_card kpi_tone_${card.tone ?? "neutral"}`;

    const label = document.createElement("p");

    label.className = "kpi_label";
    label.textContent = card.label;

    const value = document.createElement("p");

    value.className = "kpi_value";
    value.textContent = card.value;

    const hint = document.createElement("p");

    hint.className = "kpi_hint";
    hint.textContent = card.hint;

    node.appendChild(label);
    node.appendChild(value);
    node.appendChild(hint);
    mount.appendChild(node);
  }
};
