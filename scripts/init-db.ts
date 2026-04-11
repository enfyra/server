import 'reflect-metadata';
import * as dotenv from 'dotenv';
import { resolveDbTypeFromEnv } from './utils/resolve-db-type';

dotenv.config();

async function initializeDatabase(): Promise<void> {
  const dbType = resolveDbTypeFromEnv();

  if (dbType === 'mongodb') {
    const { initializeDatabaseMongo } = await import('./init-db-mongo');
    await initializeDatabaseMongo();
  } else {
    const { initializeDatabaseSql } = await import('./init-db-sql');
    await initializeDatabaseSql();
  }
}

if (require.main === module) {
  initializeDatabase()
    .then(() => {
      console.log('✅ Database initialization completed!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Database initialization failed:', error);
      process.exit(1);
    });
}

export { initializeDatabase };


