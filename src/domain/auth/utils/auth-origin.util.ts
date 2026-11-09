export function normalizeAuthOrigin(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  try {
    const parsed = new URL(value.trim());
    if ((parsed.protocol !== 'http:' && parsed.protocol !== 'https:') ||
      !parsed.host || parsed.username || parsed.password ||
      parsed.pathname !== '/' || parsed.search || parsed.hash) return null;
    return parsed.origin;
  } catch {
    return null;
  }
}
