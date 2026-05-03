import { MongoClient, ObjectId, type Db } from 'mongodb';
import { QueryBuilderService } from '@enfyra/kernel';
import { SchemaHealingService } from '../../src/engines/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_schema_healing_mongo_${Date.now()}`;

async function probeMongo(): Promise<boolean> {
  try {
    const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
    await client.connect();
    await client.close();
    return true;
  } catch {
    return false;
  }
}

function makeTableMetadata(name: string) {
  return {
    _id: new ObjectId(),
    name,
    isSystem: true,
    columns: [
      {
        name: '_id',
        type: 'ObjectId',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
      },
    ],
    relations: [],
  };
}

describe('SchemaHealingService Mongo integration', () => {
  let available = false;
  let client: MongoClient | undefined;
  let db: Db | undefined;

  beforeAll(async () => {
    available = await probeMongo();
    if (!available) return;

    DatabaseConfigService.overrideForTesting?.('mongodb');
    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);
  });

  afterAll(async () => {
    DatabaseConfigService.resetForTesting?.();
    if (!available || !client || !db) return;
    await db.dropDatabase();
    await client.close();
  });

  test('repairs persisted Mongo primary key metadata through real Mongo query builder', async () => {
    if (!available || !db) {
      console.warn('MongoDB not available, skipping real DB schema healing test');
      return;
    }

    const settingId = new ObjectId();
    const columnId = new ObjectId();

    await db.collection('setting_definition').insertOne({
      _id: settingId,
      uniquesIndexesRepaired: true,
    });
    await db.collection('column_definition').insertOne({
      _id: columnId,
      tableId: new ObjectId(),
      name: '_id',
      type: 'uuid',
      isPrimary: true,
      isGenerated: true,
      isNullable: false,
    });

    const settingTable = makeTableMetadata('setting_definition');
    const columnTable = makeTableMetadata('column_definition');
    const tables = new Map<string, any>([
      ['setting_definition', settingTable],
      ['column_definition', columnTable],
    ]);
    const queryBuilderService = new QueryBuilderService({
      mongoService: {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
        updateOne: async (collectionName: string, id: string, data: any) =>
          db.collection(collectionName).updateOne(
            { _id: typeof id === 'string' ? new ObjectId(id) : id },
            { $set: data },
          ),
        processNestedRelations: async (_tableName: string, data: any) => data,
        applyUpdateTimestamp: (data: any) => data,
      },
      databaseConfigService: {
        getDbType: () => 'mongodb',
        isMongoDb: () => true,
      },
      lazyRef: {
        metadataCacheService: {
          isLoaded: () => true,
          getMetadata: async () => ({ tables }),
        },
      },
    } as any);
    const metadataCacheService = {
      getAllTablesMetadata: async () => [],
    };

    const service = new SchemaHealingService({
      queryBuilderService,
      metadataCacheService: metadataCacheService as any,
    });

    await service.runIfNeeded();

    const repairedColumn = await db
      .collection('column_definition')
      .findOne({ _id: columnId });

    expect(repairedColumn).toMatchObject({
      _id: columnId,
      name: '_id',
      type: 'ObjectId',
      isPrimary: true,
    });
  });

  test('heals legacy Mongo junction metadata and merges existing legacy collection data', async () => {
    if (!available || !db) {
      console.warn('MongoDB not available, skipping real DB schema healing test');
      return;
    }

    await db.collection('setting_definition').deleteMany({});
    await db.collection('table_definition').deleteMany({});
    await db.collection('relation_definition').deleteMany({});

    const oldCollectionName =
      'route_definition_availableMethods_method_definition';
    try {
      await db.collection(oldCollectionName).drop();
    } catch {}

    const junction = getSqlJunctionPhysicalNames({
      sourceTable: 'route_definition',
      propertyName: 'availableMethods',
      targetTable: 'method_definition',
    });
    try {
      await db.collection(junction.junctionTableName).drop();
    } catch {}

    const routeTableId = new ObjectId();
    const methodTableId = new ObjectId();
    const owningRelationId = new ObjectId();
    const inverseRelationId = new ObjectId();
    const routeIdA = new ObjectId();
    const routeIdB = new ObjectId();
    const methodIdA = new ObjectId();
    const methodIdB = new ObjectId();

    await db.collection('setting_definition').insertOne({
      _id: new ObjectId(),
      uniquesIndexesRepaired: true,
    });
    await db.collection('table_definition').insertMany([
      { _id: routeTableId, name: 'route_definition', isSystem: true },
      { _id: methodTableId, name: 'method_definition', isSystem: true },
    ]);
    await db.collection('relation_definition').insertMany([
      {
        _id: owningRelationId,
        sourceTable: routeTableId,
        targetTable: methodTableId,
        propertyName: 'availableMethods',
        type: 'many-to-many',
        junctionTableName: oldCollectionName,
        junctionSourceColumn: 'route_definitionId',
        junctionTargetColumn: 'method_definitionId',
      },
      {
        _id: inverseRelationId,
        sourceTable: methodTableId,
        targetTable: routeTableId,
        propertyName: 'routesWithAvailable',
        type: 'many-to-many',
        mappedBy: owningRelationId,
        junctionTableName: oldCollectionName,
        junctionSourceColumn: 'method_definitionId',
        junctionTargetColumn: 'route_definitionId',
      },
    ]);
    await db.collection(oldCollectionName).insertOne({
      route_definitionId: routeIdA,
      method_definitionId: methodIdA,
    });
    await db.collection(junction.junctionTableName).insertOne({
      [junction.junctionSourceColumn]: routeIdB,
      [junction.junctionTargetColumn]: methodIdB,
    });

    const tables = new Map<string, any>([
      ['setting_definition', makeTableMetadata('setting_definition')],
      ['table_definition', makeTableMetadata('table_definition')],
      ['relation_definition', makeTableMetadata('relation_definition')],
    ]);
    const queryBuilderService = new QueryBuilderService({
      mongoService: {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
      },
      databaseConfigService: {
        getDbType: () => 'mongodb',
        isMongoDb: () => true,
      },
      lazyRef: {
        metadataCacheService: {
          isLoaded: () => true,
          getMetadata: async () => ({ tables }),
        },
      },
    } as any);
    const service = new SchemaHealingService({
      queryBuilderService,
      metadataCacheService: { getAllTablesMetadata: async () => [] } as any,
    });

    await service.runIfNeeded();

    const owningRelation = await db
      .collection('relation_definition')
      .findOne({ _id: owningRelationId });
    const inverseRelation = await db
      .collection('relation_definition')
      .findOne({ _id: inverseRelationId });
    const oldCollectionExists = await db
      .listCollections({ name: oldCollectionName })
      .toArray();
    const healedRows = await db
      .collection(junction.junctionTableName)
      .find({})
      .toArray();

    expect(owningRelation).toMatchObject({
      junctionTableName: junction.junctionTableName,
      junctionSourceColumn: junction.junctionSourceColumn,
      junctionTargetColumn: junction.junctionTargetColumn,
    });
    expect(inverseRelation).toMatchObject({
      junctionTableName: junction.junctionTableName,
      junctionSourceColumn: junction.junctionTargetColumn,
      junctionTargetColumn: junction.junctionSourceColumn,
    });
    expect(oldCollectionExists).toHaveLength(0);
    expect(healedRows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          [junction.junctionSourceColumn]: routeIdA,
          [junction.junctionTargetColumn]: methodIdA,
        }),
        expect.objectContaining({
          [junction.junctionSourceColumn]: routeIdB,
          [junction.junctionTargetColumn]: methodIdB,
        }),
      ]),
    );
  });

  test('drops orphan legacy Mongo junction collection when metadata already uses standard contract', async () => {
    if (!available || !db) {
      console.warn('MongoDB not available, skipping real DB schema healing test');
      return;
    }

    await db.collection('setting_definition').deleteMany({});
    await db.collection('table_definition').deleteMany({});
    await db.collection('relation_definition').deleteMany({});

    const oldCollectionName =
      'route_definition_availableMethods_method_definition';
    try {
      await db.collection(oldCollectionName).drop();
    } catch {}

    const junction = getSqlJunctionPhysicalNames({
      sourceTable: 'route_definition',
      propertyName: 'availableMethods',
      targetTable: 'method_definition',
    });
    try {
      await db.collection(junction.junctionTableName).drop();
    } catch {}

    const routeTableId = new ObjectId();
    const methodTableId = new ObjectId();
    const owningRelationId = new ObjectId();
    const inverseRelationId = new ObjectId();
    const routeId = new ObjectId();
    const methodId = new ObjectId();

    await db.collection('setting_definition').insertOne({
      _id: new ObjectId(),
      uniquesIndexesRepaired: true,
    });
    await db.collection('table_definition').insertMany([
      { _id: routeTableId, name: 'route_definition', isSystem: true },
      { _id: methodTableId, name: 'method_definition', isSystem: true },
    ]);
    await db.collection('relation_definition').insertMany([
      {
        _id: owningRelationId,
        sourceTable: routeTableId,
        targetTable: methodTableId,
        propertyName: 'availableMethods',
        type: 'many-to-many',
        junctionTableName: junction.junctionTableName,
        junctionSourceColumn: junction.junctionSourceColumn,
        junctionTargetColumn: junction.junctionTargetColumn,
      },
      {
        _id: inverseRelationId,
        sourceTable: methodTableId,
        targetTable: routeTableId,
        propertyName: 'routesWithAvailable',
        type: 'many-to-many',
        mappedBy: owningRelationId,
        junctionTableName: junction.junctionTableName,
        junctionSourceColumn: junction.junctionTargetColumn,
        junctionTargetColumn: junction.junctionSourceColumn,
      },
    ]);
    await db.collection(oldCollectionName).insertOne({
      route_definitionId: routeId,
      method_definitionId: methodId,
    });
    await db.collection(junction.junctionTableName).insertOne({
      [junction.junctionSourceColumn]: routeId,
      [junction.junctionTargetColumn]: methodId,
    });

    const tables = new Map<string, any>([
      ['setting_definition', makeTableMetadata('setting_definition')],
      ['table_definition', makeTableMetadata('table_definition')],
      ['relation_definition', makeTableMetadata('relation_definition')],
    ]);
    const queryBuilderService = new QueryBuilderService({
      mongoService: {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
      },
      databaseConfigService: {
        getDbType: () => 'mongodb',
        isMongoDb: () => true,
      },
      lazyRef: {
        metadataCacheService: {
          isLoaded: () => true,
          getMetadata: async () => ({ tables }),
        },
      },
    } as any);
    const service = new SchemaHealingService({
      queryBuilderService,
      metadataCacheService: { getAllTablesMetadata: async () => [] } as any,
    });

    await service.runIfNeeded();

    const oldCollectionExists = await db
      .listCollections({ name: oldCollectionName })
      .toArray();
    const healedRows = await db
      .collection(junction.junctionTableName)
      .find({})
      .toArray();

    expect(oldCollectionExists).toHaveLength(0);
    expect(healedRows).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          [junction.junctionSourceColumn]: routeId,
          [junction.junctionTargetColumn]: methodId,
        }),
      ]),
    );
  });
});
