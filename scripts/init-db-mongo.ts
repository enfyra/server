import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { MongoClient, Db } from 'mongodb';
import {
  ColumnDef,
  RelationDef,
  TableDef,
} from '../src/shared/types/database-init.types';
import {
  loadSchemaMigration,
  hasSchemaMigrations,
  applyMongoSchemaMigrations,
} from '../src/shared/utils/provision-schema-migration';
import {
  buildJunctionDefs,
  createJunctionCollections,
} from '../src/engine/mongo/utils/junction-collections';
dotenv.config();
function getBsonType(columnDef: ColumnDef): string {
  const typeMap: Record<string, string> = {
    int: 'int',
    integer: 'int',
    bigint: 'long',
    smallint: 'int',
    uuid: 'string',
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
function createValidationSchema(tableDef: TableDef, allTables: Record<string, TableDef>): any {
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
        properties[rel.propertyName] = {
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
  if (tableDef.uniques && tableDef.uniques.length > 0) {
    for (const uniqueGroup of tableDef.uniques) {
      if (Array.isArray(uniqueGroup) && uniqueGroup.length > 0) {
        const indexSpec: any = {};
        for (const fieldName of uniqueGroup) {
          indexSpec[fieldName] = 1;
        }
        const partialFilter: any = {};
        for (const fieldName of uniqueGroup) {
          partialFilter[fieldName] = { $type: 'string' };
        }
        await collection.createIndex(indexSpec, {
          unique: true,
          partialFilterExpression: partialFilter,
        });
        console.log(`  Created unique partial index on: ${uniqueGroup.join(', ')}`);
      }
    }
  }
  if (tableDef.indexes && tableDef.indexes.length > 0) {
    for (const indexGroup of tableDef.indexes) {
      if (Array.isArray(indexGroup) && indexGroup.length > 0) {
        const indexSpec: any = {};
        for (const fieldName of indexGroup) {
          indexSpec[fieldName] = 1;
        }
        await collection.createIndex(indexSpec);
        console.log(`  Created index on: ${indexGroup.join(', ')}`);
      }
    }
  }
  const dateTimeFields = tableDef.columns.filter(col =>
    col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date'
  );
  for (const field of dateTimeFields) {
    const indexName = `${collectionName}_${field.name}_idx`;
    try {
      await collection.createIndex(
        { [field.name]: -1 },
        { name: indexName }
      );
      console.log(`  Created index on datetime field: ${field.name}`);
    } catch (error: any) {
      if (error.code === 85 || error.code === 86) {
        console.log(`  Index on ${field.name} already exists, skipping`);
      } else {
        throw error;
      }
    }
  }
  const hasCreatedAt = tableDef.columns.some(col => col.name === 'createdAt');
  const hasUpdatedAt = tableDef.columns.some(col => col.name === 'updatedAt');
  if (hasCreatedAt && hasUpdatedAt) {
    const indexName = `${collectionName}_timestamps_idx`;
    try {
      await collection.createIndex(
        { createdAt: -1, updatedAt: -1 },
        { name: indexName }
      );
      console.log(`  Created compound index: createdAt + updatedAt`);
    } catch (error: any) {
      if (error.code === 85 || error.code === 86) {
        console.log(`  Compound timestamps index already exists, skipping`);
      } else {
        throw error;
      }
    }
  }
  if (tableDef.relations) {
    for (const relation of tableDef.relations) {
      if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
        const fieldName = relation.propertyName;
        const indexName = `${collectionName}_${fieldName}_fk_idx`;
        try {
          await collection.createIndex(
            { [fieldName]: 1 },
            { name: indexName }
          );
          console.log(`  Created index on M2O/O2O field: ${fieldName}`);
        } catch (error: any) {
          if (error.code === 85 || error.code === 86) {
            console.log(`  Index on ${fieldName} already exists, skipping`);
          } else {
            throw error;
          }
        }
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
        console.log(`  Backfilled ${result.modifiedCount} rows: ${tableDef.name}.${col.name} = ${col.defaultValue}`);
      }
    }
  }
}
async function createCollection(db: Db, tableDef: TableDef, allTables: Record<string, TableDef>): Promise<void> {
  const collectionName = tableDef.name;
  console.log(`📝 Creating collection: ${collectionName}`);
  const collections = await db.listCollections({ name: collectionName }).toArray();
  if (collections.length > 0) {
    console.log(`⏩ Collection already exists: ${collectionName}`);
    await backfillDefaults(db, tableDef);
    return;
  }
  const METADATA_TABLES = ['table_definition', 'column_definition', 'relation_definition'];
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
      console.log('📋 Applying schema migrations from snapshot-migration.json...');
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