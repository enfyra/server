import * as dotenv from 'dotenv';
import { resolveDbTypeFromEnv } from '../src/shared/utils/resolve-db-type';

dotenv.config();

export async function initializeDatabaseSql(): Promise<void> {
  if (resolveDbTypeFromEnv() === 'mongodb') {
    throw new Error('SQL initialization requires a SQL DB_URI.');
  }
  const { initializeDatabase } = await import('./init-db');
  await initializeDatabase();
}

if (require.main === module) {
  initializeDatabaseSql()
    .then(() => {
      console.log('✅ SQL database initialization completed!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ SQL database initialization failed:', error);
      process.exit(1);
    });
}
