import 'reflect-metadata';
import * as fs from 'fs';
import * as path from 'path';
import * as dotenv from 'dotenv';
import { MongoClient, Db } from 'mongodb';
import { MongoService } from '../src/infrastructure/mongo/services/mongo.service';
import {
  ColumnDef,
  RelationDef,
  TableDef,
} from '../src/shared/types/database-init.types';

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

  // Add columns
  for (const col of tableDef.columns) {
    if (col.isPrimary && col.name === 'id') continue;

    const bsonType = getBsonType(col);

    // If nullable, allow both the type and null
    if (col.isNullable !== false) {
      properties[col.name] = { bsonType: [bsonType, 'null'] };
    } else {
      properties[col.name] = { bsonType };
      // Only add to required if no defaultValue and not generated
      // Default values are handled in application layer (MongoService.applyDefaultValues)
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

  // Add relation fields defined in this table
  if (tableDef.relations) {
    for (const rel of tableDef.relations) {
      // Skip one-to-many (always inverse, not stored)
      if (rel.type === 'one-to-many') {
        continue;
      }

      // Store owner side relations:
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        // Single ObjectId reference
        properties[rel.propertyName] = {
          bsonType: ['objectId', 'null'],
          description: `Reference to ${rel.targetTable}`,
        };
      } else if (rel.type === 'many-to-many') {
        // Array of ObjectIds
        properties[rel.propertyName] = {
          bsonType: 'array',
          items: { bsonType: 'objectId' },
          description: `Many-to-many relation to ${rel.targetTable}`,
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
        
        // Use partial index to exclude null values from unique constraint
        // This prevents duplicate key errors when multiple documents have null values
        const partialFilter: any = {};
        for (const fieldName of uniqueGroup) {
          partialFilter[fieldName] = { $type: 'string' }; // Only index non-null values
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

  if (tableDef.relations) {
    for (const relation of tableDef.relations) {
      if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
        const fieldName = `${relation.propertyName}Id`;
        await collection.createIndex({ [fieldName]: 1 });
        console.log(`  Created index on FK: ${fieldName}`);
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
    return;
  }

  // Skip validation for metadata tables (they have dynamic fields)
  const METADATA_TABLES = ['table_definition', 'column_definition', 'relation_definition'];

  if (METADATA_TABLES.includes(collectionName)) {
    // Create without validation for metadata tables
    await db.createCollection(collectionName);
    console.log(`✅ Created collection (no validation): ${collectionName}`);
  } else {
    // Create with validation for data tables
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
  const MONGO_URI = process.env.MONGO_URI;

  if (!MONGO_URI) {
    throw new Error('MONGO_URI is not defined in environment variables');
  }

  console.log('🚀 Initializing MongoDB database...');

  const client = new MongoClient(MONGO_URI);

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');

    const dbName = MONGO_URI.match(/\/([^/?]+)(\?|$)/)?.[1] || 'enfyra';
    const db = client.db(dbName);

    const settingCollection = db.collection('setting_definition');
    const existingSettings = await settingCollection.findOne({ isInit: true });

    if (existingSettings) {
      console.log('⚠️ Database already initialized, skipping init.');
      return;
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    console.log('📖 Loaded snapshot.json');

    const tables = Object.values(snapshot) as TableDef[];
    console.log(`📊 Found ${tables.length} collections to create`);

    for (const tableDef of tables) {
      await createCollection(db, tableDef, snapshot);
    }

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

