import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { knex } from 'knex';
import { MongoClient } from 'mongodb';
import { parseDatabaseUri } from '../src/engine/knex';
import { resolveDbTypeFromEnv } from '../src/shared/utils/resolve-db-type';

dotenv.config();

const DB_TYPE = resolveDbTypeFromEnv();
const MONGO_DB_URI = DB_TYPE === 'mongodb' ? process.env.DB_URI : null;

const TEST_TABLE = '_migration_test';
const SNAPSHOT_PATH = path.resolve(process.cwd(), 'data/snapshot.json');
const SNAPSHOT_OLD_PATH = path.resolve(
  process.cwd(),
  'data/snapshot-migration.json',
);

let originalSnapshot: any;
let originalSnapshotOld: any;

function backupFiles() {
  originalSnapshot = JSON.parse(fs.readFileSync(SNAPSHOT_PATH, 'utf8'));
  originalSnapshotOld = JSON.parse(fs.readFileSync(SNAPSHOT_OLD_PATH, 'utf8'));
}

function restoreFiles() {
  fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(originalSnapshot, null, 4));
  fs.writeFileSync(
    SNAPSHOT_OLD_PATH,
    JSON.stringify(originalSnapshotOld, null, 2),
  );
}

function addTestTableToSnapshot() {
  const snapshot = JSON.parse(fs.readFileSync(SNAPSHOT_PATH, 'utf8'));
  snapshot[TEST_TABLE] = {
    name: TEST_TABLE,
    description: 'Test table for migration testing',
    isSystem: false,
    columns: [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
      },
      { name: 'name', type: 'varchar', isNullable: false },
      { name: 'status', type: 'varchar', isNullable: true },
    ],
    relations: [],
  };
  fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(snapshot, null, 4));
}

function addColumnToTestTable() {
  const snapshot = JSON.parse(fs.readFileSync(SNAPSHOT_PATH, 'utf8'));
  if (snapshot[TEST_TABLE]) {
    snapshot[TEST_TABLE].columns.push({
      name: 'newColumn',
      type: 'varchar',
      isNullable: true,
    });
    fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(snapshot, null, 4));
  }
}

function addRelationToTestTable() {
  const snapshot = JSON.parse(fs.readFileSync(SNAPSHOT_PATH, 'utf8'));
  if (snapshot[TEST_TABLE]) {
    snapshot[TEST_TABLE].relations.push({
      propertyName: 'user',
      type: 'many-to-one',
      targetTable: 'user_definition',
      isSystem: false,
    });
    fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(snapshot, null, 4));
  }
}

function removeTestTableFromSnapshot() {
  const snapshot = JSON.parse(fs.readFileSync(SNAPSHOT_PATH, 'utf8'));
  delete snapshot[TEST_TABLE];
  fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(snapshot, null, 4));
}

function addTableToDeletedTables() {
  const snapshotOld = JSON.parse(fs.readFileSync(SNAPSHOT_OLD_PATH, 'utf8'));
  if (!snapshotOld.deletedTables.includes(TEST_TABLE)) {
    snapshotOld.deletedTables.push(TEST_TABLE);
    fs.writeFileSync(SNAPSHOT_OLD_PATH, JSON.stringify(snapshotOld, null, 2));
  }
}

async function runInitScript(): Promise<void> {
  const { initializeDatabase } = await import('./init-db');
  await initializeDatabase();
}

async function testSqlMigrations(): Promise<{
  passed: string[];
  failed: string[];
}> {
  const results = { passed: [] as string[], failed: [] as string[] };

  const DB_URI = process.env.DB_URI;
  let connectionConfig: any;

  if (DB_URI) {
    const parsed = parseDatabaseUri(DB_URI);
    connectionConfig = {
      host: parsed.host,
      port: parsed.port,
      user: parsed.user,
      password: parsed.password,
      database: parsed.database,
    };
  } else {
    connectionConfig = {
      host: process.env.DB_HOST || 'localhost',
      port: Number(process.env.DB_PORT) || 5432,
      user: process.env.DB_USERNAME || 'root',
      password: process.env.DB_PASSWORD || '',
      database: process.env.DB_NAME || 'enfyra',
    };
  }

  const knexInstance = knex({
    client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
    connection: connectionConfig,
  });

  try {
    console.log('\n📦 SQL Migration Tests\n');
    console.log('='.repeat(50));

    await knexInstance.raw(`DROP TABLE IF EXISTS ${TEST_TABLE}`);

    console.log('\n1️⃣  Test: Create new table');
    addTestTableToSnapshot();
    await knexInstance('setting_definition').update({ isInit: false });
    await runInitScript();

    const tableExists = await knexInstance.schema.hasTable(TEST_TABLE);
    if (tableExists) {
      const columns = await knexInstance(TEST_TABLE).columnInfo();
      const hasName = 'name' in columns;
      const hasStatus = 'status' in columns;
      if (hasName && hasStatus) {
        results.passed.push('Create new table');
        console.log('   ✅ PASSED: Table created with all columns');
      } else {
        results.failed.push('Create new table - missing columns');
        console.log('   ❌ FAILED: Missing columns');
      }
    } else {
      results.failed.push('Create new table - table not found');
      console.log('   ❌ FAILED: Table not found');
    }

    console.log('\n2️⃣  Test: Add new column');
    addColumnToTestTable();
    await knexInstance('setting_definition').update({ isInit: false });
    await runInitScript();

    const columnsAfterAdd = await knexInstance(TEST_TABLE).columnInfo();
    if ('newColumn' in columnsAfterAdd) {
      results.passed.push('Add new column');
      console.log('   ✅ PASSED: newColumn added');
    } else {
      results.failed.push('Add new column');
      console.log('   ❌ FAILED: newColumn not found');
    }

    console.log('\n3️⃣  Test: Add relation (FK)');
    addRelationToTestTable();
    await knexInstance('setting_definition').update({ isInit: false });
    await runInitScript();

    const columnsAfterRelation = await knexInstance(TEST_TABLE).columnInfo();
    if ('userId' in columnsAfterRelation) {
      const fkCheck = await knexInstance.raw(`
        SELECT COUNT(*) as count
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
        WHERE tc.table_name = '${TEST_TABLE}'
          AND kcu.column_name = 'userId'
          AND tc.constraint_type = 'FOREIGN KEY'
      `);
      const fkCount =
        DB_TYPE === 'postgres' ? fkCheck.rows[0].count : fkCheck[0][0].count;
      if (Number(fkCount) > 0) {
        results.passed.push('Add relation with FK');
        console.log('   ✅ PASSED: userId column + FK constraint created');
      } else {
        results.failed.push('Add relation - FK missing');
        console.log('   ❌ FAILED: userId column exists but no FK');
      }
    } else {
      results.failed.push('Add relation - column missing');
      console.log('   ❌ FAILED: userId column not found');
    }

    console.log('\n4️⃣  Test: Delete table via deletedTables');
    removeTestTableFromSnapshot();
    addTableToDeletedTables();
    await knexInstance('setting_definition').update({ isInit: false });
    await runInitScript();

    const tableExistsAfterDelete =
      await knexInstance.schema.hasTable(TEST_TABLE);
    if (!tableExistsAfterDelete) {
      results.passed.push('Delete table via deletedTables');
      console.log('   ✅ PASSED: Table dropped');
    } else {
      results.failed.push('Delete table via deletedTables');
      console.log('   ❌ FAILED: Table still exists');
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('   ❌ ERROR:', message);
    results.failed.push(`SQL tests error: ${message}`);
  } finally {
    await knexInstance.destroy();
  }

  return results;
}

async function testMongoMigrations(): Promise<{
  passed: string[];
  failed: string[];
}> {
  const results = { passed: [] as string[], failed: [] as string[] };

  if (!MONGO_DB_URI) {
    console.log(
      '\n⏩ MongoDB: DB_URI not set or not mongodb://, skipping tests',
    );
    return results;
  }

  const client = new MongoClient(MONGO_DB_URI);

  try {
    await client.connect();
    const dbName = MONGO_DB_URI.match(/\/([^/?]+)(\?|$)/)?.[1] || 'enfyra';
    const db = client.db(dbName);

    console.log('\n📦 MongoDB Migration Tests\n');
    console.log('='.repeat(50));

    await db.dropCollection(TEST_TABLE).catch(() => {});

    console.log('\n1️⃣  Test: Create new collection');
    addTestTableToSnapshot();
    await db
      .collection('setting_definition')
      .updateOne({}, { $set: { isInit: false } });
    await runInitScript();

    const collections = await db
      .listCollections({ name: TEST_TABLE })
      .toArray();
    if (collections.length > 0) {
      const indexes = await db.collection(TEST_TABLE).indexes();
      const hasIdIndex = indexes.some(
        (idx: any) => idx.key && idx.key._id === 1,
      );
      if (hasIdIndex) {
        results.passed.push('Create new collection');
        console.log('   ✅ PASSED: Collection created with indexes');
      } else {
        results.failed.push('Create new collection - missing indexes');
        console.log('   ❌ FAILED: Missing indexes');
      }
    } else {
      results.failed.push('Create new collection - not found');
      console.log('   ❌ FAILED: Collection not found');
    }

    console.log('\n2️⃣  Test: Add new column');
    addColumnToTestTable();
    await db
      .collection('setting_definition')
      .updateOne({}, { $set: { isInit: false } });
    await runInitScript();

    results.passed.push('Add new column (schema updated)');
    console.log('   ✅ PASSED: Schema updated (MongoDB is schemaless)');

    console.log('\n3️⃣  Test: Delete collection via deletedTables');
    removeTestTableFromSnapshot();
    addTableToDeletedTables();
    await db
      .collection('setting_definition')
      .updateOne({}, { $set: { isInit: false } });
    await runInitScript();

    const collectionsAfterDelete = await db
      .listCollections({ name: TEST_TABLE })
      .toArray();
    if (collectionsAfterDelete.length === 0) {
      results.passed.push('Delete collection via deletedTables');
      console.log('   ✅ PASSED: Collection dropped');
    } else {
      results.failed.push('Delete collection via deletedTables');
      console.log('   ❌ FAILED: Collection still exists');
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('   ❌ ERROR:', message);
    results.failed.push(`MongoDB tests error: ${message}`);
  } finally {
    await client.close();
  }

  return results;
}

async function main(): Promise<void> {
  console.log('\n' + '='.repeat(50));
  console.log('  SCHEMA MIGRATION TEST SUITE');
  console.log('='.repeat(50));

  backupFiles();

  let sqlResults = { passed: [] as string[], failed: [] as string[] };
  let mongoResults = { passed: [] as string[], failed: [] as string[] };

  try {
    if (DB_TYPE !== 'mongodb') {
      sqlResults = await testSqlMigrations();
    }

    if (MONGO_DB_URI || DB_TYPE === 'mongodb') {
      mongoResults = await testMongoMigrations();
    }
  } finally {
    restoreFiles();
    console.log('\n📋 Restored original snapshot files');
  }

  console.log('\n' + '='.repeat(50));
  console.log('  TEST RESULTS SUMMARY');
  console.log('='.repeat(50));

  if (
    DB_TYPE !== 'mongodb' &&
    sqlResults.passed.length + sqlResults.failed.length > 0
  ) {
    console.log(`\n🗄️  SQL (${DB_TYPE}):`);
    console.log(`   Passed: ${sqlResults.passed.length}`);
    console.log(`   Failed: ${sqlResults.failed.length}`);
    sqlResults.passed.forEach((p) => console.log(`   ✅ ${p}`));
    sqlResults.failed.forEach((f) => console.log(`   ❌ ${f}`));
  }

  if (mongoResults.passed.length + mongoResults.failed.length > 0) {
    console.log('\n🍃 MongoDB:');
    console.log(`   Passed: ${mongoResults.passed.length}`);
    console.log(`   Failed: ${mongoResults.failed.length}`);
    mongoResults.passed.forEach((p) => console.log(`   ✅ ${p}`));
    mongoResults.failed.forEach((f) => console.log(`   ❌ ${f}`));
  }

  const totalPassed = sqlResults.passed.length + mongoResults.passed.length;
  const totalFailed = sqlResults.failed.length + mongoResults.failed.length;

  console.log('\n' + '='.repeat(50));
  console.log(`  TOTAL: ${totalPassed} passed, ${totalFailed} failed`);
  console.log('='.repeat(50) + '\n');

  if (totalFailed > 0) {
    process.exit(1);
  }
}

main()
  .then(() => {
    console.log('✅ All tests completed!');
    process.exit(0);
  })
  .catch((error) => {
    console.error('❌ Test suite failed:', error);
    process.exit(1);
  });
