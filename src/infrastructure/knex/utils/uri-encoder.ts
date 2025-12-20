export function encodePassword(password: string): string {
  return encodeURIComponent(password);
}

export function buildDatabaseUri(
  protocol: 'mysql' | 'postgresql' | 'postgres' | 'mongodb',
  user: string,
  password: string,
  host: string,
  port: number,
  database: string,
  options?: Record<string, string>
): string {
  const encodedUser = encodeURIComponent(user);
  const encodedPassword = encodePassword(password);
  const encodedDatabase = encodeURIComponent(database);
  
  let uri = `${protocol}://${encodedUser}:${encodedPassword}@${host}:${port}/${encodedDatabase}`;
  
  if (options && Object.keys(options).length > 0) {
    const queryString = Object.entries(options)
      .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
      .join('&');
    uri += `?${queryString}`;
  }
  
  return uri;
}

