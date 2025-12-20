export interface ParsedDatabaseUri {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

export function parseDatabaseUri(uri: string): ParsedDatabaseUri {
  try {
    const url = new URL(uri);
    
    const protocol = url.protocol.replace(':', '');
    const host = url.hostname;
    const port = url.port ? parseInt(url.port, 10) : getDefaultPort(protocol);
    const user = url.username ? decodeURIComponent(url.username) : '';
    const password = url.password ? decodeURIComponent(url.password) : '';
    const database = url.pathname ? url.pathname.replace(/^\//, '') : '';

    if (!host) {
      throw new Error('Database host is required');
    }

    if (!database) {
      throw new Error('Database name is required');
    }

    return {
      host,
      port,
      user,
      password,
      database,
    };
  } catch (error) {
    if (error instanceof TypeError) {
      throw new Error(`Invalid database URI format: ${uri}. Expected format: mysql://user:pass@host:port/database or postgresql://user:pass@host:port/database`);
    }
    throw error;
  }
}

function getDefaultPort(protocol: string): number {
  switch (protocol.toLowerCase()) {
    case 'mysql':
      return 3306;
    case 'postgresql':
    return 5432;
    case 'postgres':
      return 5432;
    default:
      throw new Error(`Unsupported database protocol: ${protocol}. Supported: mysql, postgresql, postgres`);
  }
}

