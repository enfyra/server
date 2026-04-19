export type ScriptDatabaseType = 'mysql' | 'postgres' | 'mongodb';

export function resolveDbTypeFromEnv(): ScriptDatabaseType {
  const uri = process.env.DB_URI;
  if (!uri) {
    throw new Error(
      'DB_URI environment variable is required but not set.',
    );
  }
  const protocol = new URL(uri).protocol.replace(':', '');
  switch (protocol) {
    case 'mysql':
    case 'mariadb':
      return 'mysql';
    case 'postgres':
    case 'postgresql':
      return 'postgres';
    case 'mongodb':
    case 'mongodb+srv':
      return 'mongodb';
    default:
      throw new Error(
        `Unsupported database protocol "${protocol}" in URI. Supported: mysql, postgres, mongodb.`,
      );
  }
}
