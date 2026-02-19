// Very small client-side cache helper for GET endpoints.
// Goals:
// 1) Support If-None-Match revalidation (ETag) to avoid re-downloading large payloads.
// 2) Provide a fallback payload for "degraded mode" when the API/DB is unavailable.
//
// This intentionally avoids being a general caching framework.

const PREFIX = 'bb:httpcache:v1:';

function stableStringify(obj) {
  if (!obj) return '';
  const keys = Object.keys(obj).sort();
  const out = {};
  keys.forEach((k) => {
    out[k] = obj[k];
  });
  return JSON.stringify(out);
}

export function cacheKey(url, params) {
  return PREFIX + url + ':' + stableStringify(params);
}

export function loadCache(url, params) {
  try {
    const raw = localStorage.getItem(cacheKey(url, params));
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export function saveCache(url, params, payload, etag) {
  try {
    const entry = {
      at: new Date().toISOString(),
      etag: etag || null,
      payload
    };
    localStorage.setItem(cacheKey(url, params), JSON.stringify(entry));
  } catch {
    // ignore quota errors
  }
}
