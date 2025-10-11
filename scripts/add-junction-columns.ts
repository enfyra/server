import { config } from 'dotenv';
import * as path from 'path';
config({ path: path.resolve(__dirname, '../.env') });

import knex from 'knex';

const dbKnex = knex({
  client: 'mysql2',
  connection: {
    host: process.env.DB_HOST || 'localhost',
    port: parseInt(process.env.DB_PORT!) || 3306,
    user: process.env.DB_USERNAME || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'enfyra',
  },
});

(async () => {
  console.log('Adding junction columns to relation_definition...');
  
  try {
    await dbKnex.schema.table('relation_definition', (table) => {
      table.string('junctionTableName', 255).nullable();
      table.string('junctionSourceColumn', 255).nullable();
      table.string('junctionTargetColumn', 255).nullable();
    });
    console.log('✅ Columns added successfully');
  } catch (e: any) {
    console.log('⚠️  Error:', e.message);
  }
  
  await dbKnex.destroy();
  process.exit(0);
})();



