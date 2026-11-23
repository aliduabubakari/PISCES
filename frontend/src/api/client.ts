let token = "";
export function setApiToken(value: string) {
  token = value.trim();
}
export function apiFetch(path: string, options: RequestInit = {}) {
  const headers = new Headers(options.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(path, {
    ...options,
    headers,
    signal: options.signal ?? AbortSignal.timeout(15000),
  });
}
export async function apiJSON<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await apiFetch(path, options);
  if (!response.ok) {
    if (response.status === 401)
      throw new Error("API token required or invalid.");
    let detail = `API request failed (${response.status})`;
    try {
      detail = (await response.json()).detail ?? detail;
    } catch {}
    throw new Error(
      typeof detail === "string"
        ? detail.replaceAll("_", " ")
        : "API rejected the request.",
    );
  }
  return response.status === 204 ? (undefined as T) : response.json();
}
