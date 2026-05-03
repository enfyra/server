import { MongoClient, ObjectId, type Db } from 'mongodb';
import { QueryBuilderService } from '@enfyra/kernel';
import { SchemaHealingService } from '../../src/engines/bootstrap';
import { DatabaseConfigService } from '../../src/shared/services';

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
});
