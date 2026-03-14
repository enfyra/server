import 'reflect-metadata';
import * as dotenv from 'dotenv';

dotenv.config();

async function initializeDatabase(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';

  if (DB_TYPE === 'mongodb') {
    const { initializeDatabaseMongo } = await import('./init-db-mongo');
    await initializeDatabaseMongo();
  } else if (['mysql', 'postgres', 'mariadb'].includes(DB_TYPE)) {
    const { initializeDatabaseSql } = await import('./init-db-sql');
    await initializeDatabaseSql();
  } else {
    throw new Error(`Unsupported DB_TYPE: ${DB_TYPE}. Supported: mysql, postgres, mariadb, mongodb`);
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


