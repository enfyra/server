import { MongoClient } from 'mongodb';
import * as fs from 'fs';
import * as path from 'path';

async function dropDatabase() {
  const envPath = path.join(process.cwd(), '.env');
  const envContent = fs.readFileSync(envPath, 'utf8');

  const dbUriMatch = envContent.match(/DB_URI=(mongodb(?:\+srv)?:\/\/[^\s]+)/);
  if (!dbUriMatch) {
    console.error('DB_URI not found in .env file');
    process.exit(1);
  }

  const uri = dbUriMatch[1];
  const client = new MongoClient(uri);

  try {
    await client.connect();

    const uriObj = new URL(uri);
    const dbName = uriObj.pathname.slice(1);

    console.log(`Dropping database: ${dbName}`);
    const db = client.db(dbName);

    await db.dropDatabase();
    console.log(`✅ Database "${dbName}" dropped successfully`);
  } catch (error) {
    console.error('❌ Error dropping database:', error);
    process.exit(1);
  } finally {
    await client.close();
  }
}

dropDatabase();
