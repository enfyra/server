import * as dotenv from 'dotenv';
import { resolveDbTypeFromEnv } from '../src/shared/utils/resolve-db-type';

dotenv.config();

export async function initializeDatabaseMongo(): Promise<void> {
  if (resolveDbTypeFromEnv() !== 'mongodb') {
    throw new Error('MongoDB initialization requires a MongoDB DB_URI.');
  }
  const { initializeDatabase } = await import('./init-db');
  await initializeDatabase();
}

if (require.main === module) {
  initializeDatabaseMongo()
    .then(() => {
      console.log('✅ MongoDB initialization completed!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ MongoDB initialization failed:', error);
      process.exit(1);
    });
}
