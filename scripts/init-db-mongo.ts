import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { MongoClient, type Db } from 'mongodb';
import {
  type ColumnDef,
  type TableDef,
} from '../src/shared/types/database-init.types';
import {
  loadSchemaMigration,
  hasSchemaMigrations,
  applyMongoSchemaMigrations,
} from '../src/shared/utils/provision-schema-migration';
import {
  buildJunctionDefs,
  buildMongoFullIndexSpecs,
  createJunctionCollections,
  getMongoStoredRelationField,
} from '../src/engines/mongo';
dotenv.config();
function getBsonType(columnDef: ColumnDef): string {
  const typeMap: Record<string, string> = {
    int: 'int',
    integer: 'int',
    bigint: 'long',
    smallint: 'int',
    uuid: 'string',
    objectId: 'objectId',
    ObjectId: 'objectId',
    objectid: 'objectId',
    varchar: 'string',
    text: 'string',
    boolean: 'bool',
    bool: 'bool',
    date: 'date',
    datetime: 'date',
    timestamp: 'date',
    'simple-json': 'object',
    richtext: 'string',
    code: 'string',
    'array-select': 'array',
    enum: 'string',
  };
  return typeMap[columnDef.type] || 'string';
}
function createValidationSchema(
  tableDef: TableDef,
  _allTables: Record<string, TableDef>,
): any {
  const properties: any = {};
  const required: string[] = [];
  for (const col of tableDef.columns) {
    if (col.isPrimary && col.name === 'id') continue;
    const bsonType = getBsonType(col);
    if (col.isNullable !== false) {
      properties[col.name] = { bsonType: [bsonType, 'null'] };
    } else {
      properties[col.name] = { bsonType };
      if (col.defaultValue === undefined && !col.isGenerated) {
        required.push(col.name);
      }
    }
    if (col.type === 'enum' && Array.isArray(col.options)) {
      properties[col.name].enum = col.options;
    }
    if (col.description) {
      properties[col.name].description = col.description;
    }
  }
  if (tableDef.relations) {
    for (const rel of tableDef.relations) {
      if (rel.type === 'one-to-many') {
        continue;
      }
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const fieldName = getMongoStoredRelationField(rel) || rel.propertyName;
        properties[fieldName] = {
          bsonType: ['objectId', 'null'],
          description: `Reference to ${rel.targetTable}`,
        };
      }
    }
  }
  const schema: any = {
    $jsonSchema: {
      bsonType: 'object',
      properties,
    },
  };
  if (required.length > 0) {
    schema.$jsonSchema.required = required;
  }
  return schema;
}
async function createIndexes(
  db: Db,
  collectionName: string,
  tableDef: TableDef,
): Promise<void> {
  const collection = db.collection(collectionName);
  const specs = buildMongoFullIndexSpecs({
    collectionName,
    columns: tableDef.columns || [],
    uniques: tableDef.uniques || [],
    indexes: tableDef.indexes || [],
    relations: tableDef.relations || [],
  });
  for (const spec of specs) {
    try {
      await collection.createIndex(spec.keys, spec.options);
      console.log(`  Created index: ${spec.name}`);
    } catch (error: any) {
      if (error.code === 85 || error.code === 86) {
        console.log(`  Index ${spec.name} already exists, skipping`);
      } else {
        throw error;
      }
    }
  }
}
async function backfillDefaults(db: Db, tableDef: TableDef): Promise<void> {
  const collection = db.collection(tableDef.name);
  for (const col of tableDef.columns) {
    if (col.defaultValue !== undefined && col.defaultValue !== null) {
      const result = await collection.updateMany(
        { [col.name]: { $exists: false } },
        { $set: { [col.name]: col.defaultValue } },
      );
      if (result.modifiedCount > 0) {
        console.log(
          `  Backfilled ${result.modifiedCount} rows: ${tableDef.name}.${col.name} = ${col.defaultValue}`,
        );
      }
    }
  }
}
async function createCollection(
  db: Db,
  tableDef: TableDef,
  allTables: Record<string, TableDef>,
): Promise<void> {
  const collectionName = tableDef.name;
  console.log(`📝 Creating collection: ${collectionName}`);
  const collections = await db
    .listCollections({ name: collectionName })
    .toArray();
  if (collections.length > 0) {
    console.log(`⏩ Collection already exists: ${collectionName}`);
    await backfillDefaults(db, tableDef);
    return;
  }
  const METADATA_TABLES = [
    'table_definition',
    'column_definition',
    'relation_definition',
  ];
  if (METADATA_TABLES.includes(collectionName)) {
    await db.createCollection(collectionName);
    console.log(`✅ Created collection (no validation): ${collectionName}`);
  } else {
    const validationSchema = createValidationSchema(tableDef, allTables);
    await db.createCollection(collectionName, {
      validator: validationSchema,
      validationLevel: 'moderate',
      validationAction: 'error',
    });
    console.log(`✅ Created collection (with validation): ${collectionName}`);
  }
  await createIndexes(db, collectionName, tableDef);
}
export async function initializeDatabaseMongo(): Promise<void> {
  const DB_URI = process.env.DB_URI;
  if (!DB_URI) {
    throw new Error('DB_URI is not defined in environment variables');
  }
  console.log('🚀 Initializing MongoDB database...');
  const client = new MongoClient(DB_URI);
  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');
    const dbName = DB_URI.match(/\/([^/?]+)(\?|$)/)?.[1] || 'enfyra';
    const db = client.db(dbName);
    const settingCollection = db.collection('setting_definition');
    const existingSettings = await settingCollection.findOne({ isInit: true });
    if (existingSettings) {
      console.log('⚠️ Database already initialized, skipping init.');
      return;
    }

    const schemaMigration = loadSchemaMigration();

    // Apply schema migrations (dangerous operations: remove, modify)
    if (schemaMigration && hasSchemaMigrations(schemaMigration)) {
      console.log(
        '📋 Applying schema migrations from snapshot-migration.json...',
      );
      await applyMongoSchemaMigrations(db, schemaMigration);
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
    console.log('📖 Loaded snapshot.json');
    const tables = Object.values(snapshot) as TableDef[];
    console.log(`📊 Found ${tables.length} collections to create`);
    for (const tableDef of tables) {
      await createCollection(db, tableDef, snapshot);
    }

    const junctionDefs = buildJunctionDefs(snapshot);
    await createJunctionCollections(db, junctionDefs);

    console.log('🎉 MongoDB database initialization completed!');
  } catch (error) {
    console.error('❌ Error during MongoDB initialization:', error);
    throw error;
  } finally {
    await client.close();
  }
}
if (require.main === module) {
  initializeDatabaseMongo()
    .then(() => {
      console.log('✅ Done!');
      process.exit(0);
    })
    .catch((error) => {
      console.error('❌ Failed:', error);
      process.exit(1);
    });
}
