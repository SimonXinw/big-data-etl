/**
 * 数据获取：优先 Supabase PostgREST（HTTPS），回退直连 Postgres（同源 /api/rows）。
 */

const defaultRowsLimit = 500;

const clampLimit = (limit) => {
  const parsed = Number(limit ?? defaultRowsLimit);

  if (!Number.isFinite(parsed) || parsed < 1) {
    return defaultRowsLimit;
  }

  return Math.min(Math.floor(parsed), 2000);
};

const buildPostgresRowsUrl = (schema, tableName, limit) => {
  const params = new URLSearchParams({
    schema,
    table: tableName,
    limit: String(clampLimit(limit)),
  });

  return `/api/rows?${params.toString()}`;
};

const buildSupabaseProxyRowsUrl = (schema, tableName, limit) => {
  const params = new URLSearchParams({
    schema,
    table: tableName,
    limit: String(clampLimit(limit)),
  });

  return `/api/supabase-rows?${params.toString()}`;
};

export const fetchCatalog = async () => {
  const response = await fetch("/api/catalog");

  if (!response.ok) {
    throw new Error(`catalog HTTP ${response.status}`);
  }

  return response.json();
};

export const fetchClientConfig = async () => {
  const response = await fetch("/api/client-config");

  if (!response.ok) {
    throw new Error(`client-config HTTP ${response.status}`);
  }

  return response.json();
};

export const fetchRowsPostgres = async (schema, tableName, limit) => {
  const response = await fetch(buildPostgresRowsUrl(schema, tableName, limit));

  if (!response.ok) {
    const text = await response.text();

    throw new Error(text || `rows HTTP ${response.status}`);
  }

  return response.json();
};

export const fetchRowsSupabaseProxy = async (schema, tableName, limit) => {
  const response = await fetch(buildSupabaseProxyRowsUrl(schema, tableName, limit));

  if (!response.ok) {
    const text = await response.text();

    throw new Error(text || `supabase-rows HTTP ${response.status}`);
  }

  return response.json();
};

export const fetchRowsSupabaseBrowser = async (supabaseUrl, anonKey, schema, tableName, limit) => {
  const base = supabaseUrl.replace(/\/$/, "");
  const path = `/rest/v1/${encodeURIComponent(tableName)}`;
  const url = new URL(`${base}${path}`);

  url.searchParams.set("select", "*");
  url.searchParams.set("limit", String(clampLimit(limit)));

  const response = await fetch(url.toString(), {
    headers: {
      apikey: anonKey,
      Authorization: `Bearer ${anonKey}`,
      Accept: "application/json",
      "Accept-Profile": schema,
    },
  });

  if (!response.ok) {
    const text = await response.text();

    throw new Error(text || `Supabase REST HTTP ${response.status}`);
  }

  const rows = await response.json();

  if (!Array.isArray(rows)) {
    throw new Error("Supabase REST 返回了非数组 JSON");
  }

  const columns = rows.length ? Object.keys(rows[0]) : [];

  return {
    schema,
    table: tableName,
    limit: clampLimit(limit),
    row_count: rows.length,
    columns,
    rows,
    source: "supabase_rest_browser",
  };
};

export const fetchDataset = async (clientConfig, schema, tableName, limit) => {
  const resolved = clientConfig ?? {};

  const browserEnabled = Boolean(
    resolved.client_supabase_fetch_enabled && resolved.supabase_url && resolved.supabase_anon_key,
  );

  if (browserEnabled) {
    return fetchRowsSupabaseBrowser(
      String(resolved.supabase_url),
      String(resolved.supabase_anon_key),
      schema,
      tableName,
      limit,
    );
  }

  if (resolved.supabase_configured) {
    return fetchRowsSupabaseProxy(schema, tableName, limit);
  }

  return fetchRowsPostgres(schema, tableName, limit);
};

export const fetchHealth = async () => {
  const response = await fetch("/api/health");

  if (!response.ok) {
    throw new Error(`health HTTP ${response.status}`);
  }

  return response.json();
};
