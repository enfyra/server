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

function createValidationSchema(tableDef: TableDef): any {
  const properties: any = {};
  const required: string[] = [];

  for (const col of tableDef.columns) {
    if (col.isPrimary && col.name === 'id') continue;

    const bsonType = getBsonType(col);
    properties[col.name] = { bsonType };

    if (col.isNullable === false && !col.isGenerated) {
      required.push(col.name);
    }

    if (col.type === 'enum' && Array.isArray(col.options)) {
      properties[col.name].enum = col.options;
    }

    if (col.description) {
      properties[col.name].description = col.description;
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

/**
 * Insert metadata from snapshot into table_definition, column_definition, relation_definition
 */
async function insertMetadata(db: Db, tables: TableDef[]): Promise<void> {
  const tableNameToId: Record<string, any> = {};
  const columnIdMap: Record<string, any[]> = {}; // tableName -> column ObjectIds
  const relationIdMap: Record<string, any[]> = {}; // tableName -> relation ObjectIds
  
  // Phase 1: Insert table definitions (without columns/relations)
  console.log('  📝 Phase 1: Inserting table definitions...');
  for (const tableDef of tables) {
    const def = tableDef as any;
    const data = MongoService.applyTimestampsStatic({
      name: def.name,
      isSystem: def.isSystem || false,
      alias: def.alias || null,
      description: def.description || null,
      uniques: JSON.stringify(def.uniques || []),
      indexes: JSON.stringify(def.indexes || []),
    });
    const result = await db.collection('table_definition').insertOne(data);
    tableNameToId[def.name] = result.insertedId;
    console.log(`    ✅ ${def.name}`);
  }
  
  // Phase 2: Insert column definitions and collect ObjectIds
  console.log('  📝 Phase 2: Inserting column definitions...');
  for (const tableDef of tables) {
    const tableId = tableNameToId[tableDef.name];
    if (!tableId) continue;
    
    const columnIds = [];
    for (const col of tableDef.columns) {
      // For MongoDB: primary key column must have name="_id" and type="uuid"
      const columnName = col.isPrimary ? '_id' : col.name;
      const columnType = col.isPrimary ? 'uuid' : col.type;
      
      const data = MongoService.applyTimestampsStatic({
        name: columnName,
        type: columnType,
        isPrimary: col.isPrimary || false,
        isGenerated: col.isGenerated || false,
        isNullable: col.isNullable ?? true,
        isSystem: col.isSystem || false,
        isUpdatable: col.isUpdatable ?? true,
        isHidden: col.isHidden || false,
        defaultValue: col.defaultValue ? JSON.stringify(col.defaultValue) : null,
        options: col.options ? JSON.stringify(col.options) : null,
        description: col.description || null,
        placeholder: col.placeholder || null,
        table: tableId, // Store table ObjectId reference
      });
      const result = await db.collection('column_definition').insertOne(data);
      columnIds.push(result.insertedId);
    }
    columnIdMap[tableDef.name] = columnIds;
    console.log(`    ✅ ${tableDef.name}: ${tableDef.columns.length} columns`);
  }
  
  // Phase 3: Insert relation definitions and collect ObjectIds
  console.log('  📝 Phase 3: Inserting relation definitions...');
  for (const tableDef of tables) {
    const tableId = tableNameToId[tableDef.name];
    if (!tableId || !tableDef.relations) continue;
    
    const relationIds = [];
    for (const rel of tableDef.relations) {
      const relDef = rel as any;
      const targetId = tableNameToId[relDef.targetTable];
      if (!targetId) continue;
      
      const data = MongoService.applyTimestampsStatic({
        propertyName: relDef.propertyName,
        type: relDef.type,
        sourceTable: tableId, // ObjectId reference to source table
        targetTable: targetId, // ObjectId reference to target table
        inversePropertyName: relDef.inversePropertyName || null,
        isNullable: relDef.isNullable ?? true,
        isSystem: relDef.isSystem || false,
        description: relDef.description || null,
      });
      const result = await db.collection('relation_definition').insertOne(data);
      relationIds.push(result.insertedId);
    }
    relationIdMap[tableDef.name] = relationIds;
    if (relationIds.length > 0) {
      console.log(`    ✅ ${tableDef.name}: ${relationIds.length} relations`);
    }
  }
  
  // Phase 4: Update table_definition with column and relation ObjectIds
  console.log('  📝 Phase 4: Updating table_definition with column/relation ObjectIds...');
  for (const tableDef of tables) {
    const tableId = tableNameToId[tableDef.name];
    if (!tableId) continue;
    
    const updateData: any = {};
    
    // Add column ObjectIds if any
    if (columnIdMap[tableDef.name] && columnIdMap[tableDef.name].length > 0) {
      updateData.columns = columnIdMap[tableDef.name];
    }
    
    // Add relation ObjectIds if any
    if (relationIdMap[tableDef.name] && relationIdMap[tableDef.name].length > 0) {
      updateData.relations = relationIdMap[tableDef.name];
    }
    
    if (Object.keys(updateData).length > 0) {
      await db.collection('table_definition').updateOne(
        { _id: tableId },
        { $set: updateData }
      );
      console.log(`    ✅ ${tableDef.name}: Updated with ${updateData.columns?.length || 0} columns, ${updateData.relations?.length || 0} relations`);
    }
  }
  
  console.log('✅ Metadata insertion completed!');
}

async function createCollection(db: Db, tableDef: TableDef): Promise<void> {
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
    const validationSchema = createValidationSchema(tableDef);
    
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
      await createCollection(db, tableDef);
    }

    // Insert metadata into table_definition, column_definition, relation_definition
    console.log('📝 Inserting metadata into collections...');
    await insertMetadata(db, tables);

    // Insert setting_definition from init.json first
    console.log('⚙️ Inserting setting_definition...');
    const initJsonPath = path.join(process.cwd(), 'src/core/bootstrap/data/init.json');
    const initJson = JSON.parse(fs.readFileSync(initJsonPath, 'utf8'));
    const settingData = initJson.setting_definition;
    
    await settingCollection.updateOne(
      {},
      { 
        $set: { 
          ...settingData,
          isInit: true, // Override isInit to true after initialization
        } 
      },
      { upsert: true }
    );

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

