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
    const nullable = col.isNullable !== false; // default nullable when not specified
    properties[col.name] = { bsonType: nullable ? [bsonType, 'null'] : bsonType };

    // Only mark required when explicitly non-nullable and not generated and no defaultValue
    if (!nullable && !col.isGenerated && (col as any).defaultValue == null) {
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
async function upsertMetadataMongo(db: Db, tables: TableDef[]): Promise<void> {
  const tableNameToId: Record<string, any> = {};
  const columnIdMap: Record<string, any[]> = {}; // tableName -> column ObjectIds
  const relationIdMap: Record<string, any[]> = {}; // tableName -> relation ObjectIds
  
  // Provide sane defaults so missing fields in snapshot are still present in DB
  const TABLE_DEFAULTS: any = {
    alias: null as any,
    description: null as any,
    isSystem: false,
    uniques: [],
    indexes: [],
    fullTextIndexes: [],
  };
  const COLUMN_DEFAULTS: any = {
    isPrimary: false,
    isGenerated: false,
    isNullable: true,
    isSystem: false,
    isUpdatable: true,
    isHidden: false,
    defaultValue: null as any,
    options: null as any,
    description: null as any,
    placeholder: null as any,
  };
  const RELATION_DEFAULTS: any = {
    inversePropertyName: null as any,
    isNullable: true,
    isSystem: false,
    description: null as any,
  };
  
  const applyDefaults = (obj: any, defaults: any) => {
    for (const [k, v] of Object.entries(defaults)) {
      if (obj[k] === undefined) {
        obj[k] = v;
      }
    }
    return obj;
  };
  
  // Phase 1: Upsert table definitions
  console.log('  📝 Phase 1: Upserting table definitions...');
  for (const tableDef of tables) {
    const def = tableDef as any;
    const data: any = {
      name: def.name,
    };
    
    // Dynamic: Add all properties from snapshot
    for (const [key, value] of Object.entries(def)) {
      if (key === 'name') continue; // Skip name, already set above
      if (key === 'columns') continue; // Skip columns, handled separately
      if (key === 'relations') continue; // Skip relations, handled separately
      
      // For MongoDB, keep arrays/objects as-is (no stringify needed)
      data[key] = value;
    }
    // Ensure defaults for missing fields
    applyDefaults(data, TABLE_DEFAULTS);
    
    // Check if document exists
    const existing = await db.collection('table_definition').findOne({ name: def.name });
    
    if (existing) {
      // Update existing document with new fields only
      const updateData = { ...data, updatedAt: new Date() };
      console.log(`    🔄 Updating ${def.name} with:`, JSON.stringify(updateData, null, 2));
      await db.collection('table_definition').updateOne(
        { name: def.name },
        { $set: updateData }
      );
      tableNameToId[def.name] = existing._id;
      console.log(`    ✅ Updated ${def.name}`);
    } else {
      // Insert new document with full timestamps
      const finalData = MongoService.applyTimestampsStatic(data);
      const result = await db.collection('table_definition').insertOne(finalData);
      tableNameToId[def.name] = result.insertedId;
      console.log(`    ✅ Created ${def.name}`);
    }
  }
  
  // Phase 2: Upsert column definitions
  console.log('  📝 Phase 2: Upserting column definitions...');
  for (const tableDef of tables) {
    const tableId = tableNameToId[tableDef.name];
    if (!tableId) continue;
    
    // Delete existing columns for this table (Mongo uses 'table' ObjectId)
    await db.collection('column_definition').deleteMany({ table: tableId });
    
    const columnIds = [];
    for (const col of tableDef.columns) {
      // Mongo-specific: relation_definition should NOT have SQL-style FK columns
      if (tableDef.name === 'relation_definition' && (col.name === 'sourceTableId' || col.name === 'targetTableId')) {
        continue;
      }
      const data: any = {
        table: tableId, // Mongo metadata uses 'table' field
      };
      
      // Dynamic: Add all properties from column
      for (const [key, value] of Object.entries(col)) {
        // For MongoDB: primary key column must have name="_id" and type="uuid"
        if (key === 'name' && col.isPrimary) {
          data.name = '_id';
        } else if (key === 'type' && col.isPrimary) {
          data.type = 'uuid';
        } else {
          // For MongoDB, keep arrays/objects as-is (no stringify needed)
          data[key] = value;
        }
      }
      // Ensure defaults for missing column fields
      applyDefaults(data, COLUMN_DEFAULTS);
      
      const finalData = MongoService.applyTimestampsStatic(data);
      const result = await db.collection('column_definition').insertOne(finalData);
      columnIds.push(result.insertedId);
    }
    columnIdMap[tableDef.name] = columnIds;
    console.log(`    ✅ Updated columns for ${tableDef.name}`);
    
    // Optional: Keep table_definition.columns up to date for convenience
    await db.collection('table_definition').updateOne(
      { _id: tableId },
      { $set: { columns: columnIds } },
    );
  }
  
  // Phase 3: Upsert relation definitions
  console.log('  📝 Phase 3: Upserting relation definitions...');
  for (const tableDef of tables) {
    const tableId = tableNameToId[tableDef.name];
    if (!tableId) continue;
    
    // Delete existing relations for this table (Mongo uses 'sourceTable' ObjectId)
    await db.collection('relation_definition').deleteMany({ sourceTable: tableId });
    
    const relationIds = [];
    if (tableDef.relations) {
      for (const rel of tableDef.relations) {
        const targetTableId = tableNameToId[rel.targetTable];
        if (!targetTableId) continue;
        
        const data: any = {
          sourceTable: tableId, // Mongo metadata uses 'sourceTable'
          targetTable: targetTableId, // Mongo metadata uses 'targetTable'
        };
        
        // Dynamic: Add all properties from relation
        for (const [key, value] of Object.entries(rel)) {
          if (key === 'targetTable') continue; // Skip, handled above
          // For MongoDB, keep arrays/objects as-is (no stringify needed)
          data[key] = value;
        }
        // Ensure defaults for missing relation fields
        applyDefaults(data, RELATION_DEFAULTS);
        
        const finalData = MongoService.applyTimestampsStatic(data);
        const result = await db.collection('relation_definition').insertOne(finalData);
        relationIds.push(result.insertedId);
      }
    }
    relationIdMap[tableDef.name] = relationIds;
    console.log(`    ✅ Updated relations for ${tableDef.name}`);
    
    // Optional: Keep table_definition.relations up to date for convenience
    await db.collection('table_definition').updateOne(
      { _id: tableId },
      { $set: { relations: relationIds } },
    );
  }
  
  console.log('✅ MongoDB metadata upsert completed!');
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

async function updateValidationForExistingCollections(db: Db, tables: TableDef[]): Promise<void> {
  const METADATA_TABLES = ['table_definition', 'column_definition', 'relation_definition'];
  for (const tableDef of tables) {
    const collectionName = tableDef.name;
    if (METADATA_TABLES.includes(collectionName)) continue;
    const collections = await db.listCollections({ name: collectionName }).toArray();
    if (collections.length === 0) continue;
    try {
      const validationSchema = createValidationSchema(tableDef);
      await db.command({
        collMod: collectionName,
        validator: validationSchema,
        validationLevel: 'moderate',
        validationAction: 'error',
      });
      console.log(`🔧 Updated validation for existing collection: ${collectionName}`);
    } catch (err: any) {
      console.warn(`⚠️ Failed to update validation for ${collectionName}: ${err?.message}`);
    }
  }
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
    const existingSettings = await settingCollection.findOne({});

    if (existingSettings?.isInit === true) {
      console.log('⚠️ Database already initialized, skipping init.');
      return;
    } else if (existingSettings?.isInit === false) {
      console.log('🔄 Database exists but not initialized, will upsert data...');
    }

    const snapshotPath = path.resolve(process.cwd(), 'data/snapshot.json');
    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));

    console.log('📖 Loaded snapshot.json');


    const tables = Object.values(snapshot) as TableDef[];
    console.log(`📊 Found ${tables.length} collections to create`);

    for (const tableDef of tables) {
      await createCollection(db, tableDef);
    }

    // Upsert metadata into table_definition, column_definition, relation_definition
    console.log('📝 Upserting metadata into collections...');
    await upsertMetadataMongo(db, tables);

    // Ensure validation schemas reflect latest isNullable rules
    await updateValidationForExistingCollections(db, tables);

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

