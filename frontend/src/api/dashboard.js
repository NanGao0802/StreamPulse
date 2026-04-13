const API_BASE = (import.meta.env.VITE_API_BASE || "http://127.0.0.1:18000").replace(/\/$/, "");

function buildUrl(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.append(key, value);
    }
  });
  return url.toString();
}

async function request(path, params = {}) {
  const url = buildUrl(path, params);
  console.log("[API REQUEST]", url);

  const res = await fetch(url, {
    method: "GET",
  });

  const text = await res.text();

  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${text}`);
  }

  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`JSON parse error: ${text}`);
  }
}

export function getTopics() {
  return request("/api/topics");
}

export function getDashboard(params = {}) {
  return request("/api/dashboard", params);
}

export function getComments(params = {}) {
  return request("/api/comments", params);
}

export function getAlerts(params = {}) {
  return request("/api/alerts", params);
}

export function getApiBase() {
  return API_BASE;
}
