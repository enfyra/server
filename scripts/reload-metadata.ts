import 'reflect-metadata';
import * as dotenv from 'dotenv';
import { Knex, knex } from 'knex';

dotenv.config();

async function reloadMetadata(): Promise<void> {
  const DB_TYPE = process.env.DB_TYPE || 'mysql';
  const DB_HOST = process.env.DB_HOST || 'localhost';
  const DB_PORT = Number(process.env.DB_PORT) || (DB_TYPE === 'postgres' ? 5432 : 3306);
  const DB_USERNAME = process.env.DB_USERNAME || 'root';
  const DB_PASSWORD = process.env.DB_PASSWORD || '';
  const DB_NAME = process.env.DB_NAME || 'enfyra';

  const knexInstance = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: DB_HOST,
      port: DB_PORT,
      user: DB_USERNAME,
      password: DB_PASSWORD,
      database: DB_NAME,
    },
  });

  try {
    console.log('🔄 Reloading metadata cache...');

    // Clear Redis cache
    const redis = require('redis');
    const client = redis.createClient({
      url: process.env.REDIS_URI || 'redis://localhost:6379'
    });
    
    await client.connect();
    await client.del('enfyra:metadata:cache');
    await client.del('enfyra:metadata:version');
    console.log('✅ Cleared Redis metadata cache');
    
    await client.disconnect();

    // Trigger metadata reload by updating a table
    await knexInstance('table_definition')
      .where('name', 'menu_definition')
      .update({ updatedAt: knexInstance.fn.now() });
    
    console.log('✅ Updated table_definition to trigger reload');

  } catch (error) {
    console.error('❌ Error reloading metadata:', error);
    throw error;
  } finally {
    await knexInstance.destroy();
  }
}

// For direct execution
if (require.main === module) {
  reloadMetadata()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}

export { reloadMetadata };

