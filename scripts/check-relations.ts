import 'reflect-metadata';
import * as dotenv from 'dotenv';
import { Knex, knex } from 'knex';

dotenv.config();

async function checkRelations(): Promise<void> {
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
    console.log('🔍 Checking relations in database...');

    // Check if relation_definition table exists
    const hasTable = await knexInstance.schema.hasTable('relation_definition');
    console.log(`📊 relation_definition table exists: ${hasTable}`);

    if (hasTable) {
      // Check total count
      const count = await knexInstance('relation_definition').count('* as count').first();
      console.log(`📊 Total relations: ${count?.count}`);

      // Check sidebar relations
      const sidebarRelations = await knexInstance('relation_definition')
        .where('propertyName', 'sidebar')
        .select('*');
      
      console.log(`📊 Sidebar relations: ${sidebarRelations.length}`);
      if (sidebarRelations.length > 0) {
        console.log('📋 Sidebar relations:', sidebarRelations);
      }

      // Check menu_definition relations
      const menuTable = await knexInstance('table_definition')
        .where('name', 'menu_definition')
        .first();
      
      if (menuTable) {
        console.log(`📊 menu_definition table ID: ${menuTable.id}`);
        
        const menuRelations = await knexInstance('relation_definition')
          .where('sourceTableId', menuTable.id)
          .select('*');
        
        console.log(`📊 menu_definition relations: ${menuRelations.length}`);
        if (menuRelations.length > 0) {
          console.log('📋 menu_definition relations:', menuRelations);
        }
      }
    }

  } catch (error) {
    console.error('❌ Error checking relations:', error);
    throw error;
  } finally {
    await knexInstance.destroy();
  }
}

// For direct execution
if (require.main === module) {
  checkRelations()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}

export { checkRelations };
