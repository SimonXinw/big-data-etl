/**
 * 将 JSON 行渲染为可滚动表格。
 */

const formatCell = (value) => {
  if (value === null || value === undefined) {
    return "";
  }

  if (typeof value === "object") {
    return JSON.stringify(value);
  }

  return String(value);
};

export const renderTable = (mount, columns, rows) => {
  if (!mount) {
    return;
  }

  mount.innerHTML = "";

  if (!columns?.length) {
    const empty = document.createElement("div");

    empty.className = "empty_state";
    empty.innerHTML = "<strong>暂无列信息</strong><span>请确认已选择表并成功拉取数据。</span>";
    mount.appendChild(empty);

    return;
  }

  if (!rows?.length) {
    const empty = document.createElement("div");

    empty.className = "empty_state";
    empty.innerHTML = "<strong>暂无数据行</strong><span>该表可能尚未被 ETL 写入，可先跑发布命令。</span>";
    mount.appendChild(empty);

    return;
  }

  const scroll = document.createElement("div");

  scroll.className = "table_scroll";

  const table = document.createElement("table");

  table.className = "data_grid";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");

  for (const column of columns) {
    const th = document.createElement("th");

    th.textContent = column;
    headRow.appendChild(th);
  }

  thead.appendChild(headRow);

  const tbody = document.createElement("tbody");

  for (const row of rows) {
    const tr = document.createElement("tr");

    for (const column of columns) {
      const td = document.createElement("td");

      td.textContent = formatCell(row[column]);
      tr.appendChild(td);
    }

    tbody.appendChild(tr);
  }

  table.appendChild(thead);
  table.appendChild(tbody);
  scroll.appendChild(table);
  mount.appendChild(scroll);
};
