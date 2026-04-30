export function parseCookie(cookieHeader: string): Record<string, string> {
  return Object.fromEntries(
    cookieHeader.split(';').map((c) => {
      const [key, value] = c.trim().split('=');
      return [key, decodeURIComponent(value)];
    }),
  );
}
