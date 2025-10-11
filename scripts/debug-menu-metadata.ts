import 'reflect-metadata';
import * as dotenv from 'dotenv';
import { Knex, knex } from 'knex';

dotenv.config();

async function debugMenuMetadata(): Promise<void> {
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
    console.log('🔍 Debugging menu_definition metadata...');

    // Get table metadata
    const menuTable = await knexInstance('table_definition')
      .where('name', 'menu_definition')
      .first();
    
    console.log('📊 Table metadata:', JSON.stringify(menuTable, null, 2));

    // Get columns
    const columns = await knexInstance('column_definition')
      .where('tableId', menuTable.id)
      .select('*');
    
    console.log('📊 Columns:', JSON.stringify(columns, null, 2));

    // Get relations
    const relations = await knexInstance('relation_definition')
      .where('sourceTableId', menuTable.id)
      .select('*');
    
    console.log('📊 Relations:', JSON.stringify(relations, null, 2));

    // Test lookupFieldOrRelation logic
    const mockMeta = {
      name: menuTable.name,
      columns: columns,
      relations: relations
    };

    console.log('📊 Mock metadata structure:', JSON.stringify(mockMeta, null, 2));

    // Test lookup for 'sidebar'
    const sidebarRelation = mockMeta.relations?.find((rel: any) => rel.propertyName === 'sidebar');
    console.log('📊 Sidebar relation found:', !!sidebarRelation);
    if (sidebarRelation) {
      console.log('📊 Sidebar relation details:', JSON.stringify(sidebarRelation, null, 2));
    }

  } catch (error) {
    console.error('❌ Error debugging metadata:', error);
    throw error;
  } finally {
    await knexInstance.destroy();
  }
}

// For direct execution
if (require.main === module) {
  debugMenuMetadata()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}

export { debugMenuMetadata };

