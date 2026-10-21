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
    const client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 2000,
    });
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
        type: 'objectId',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
      },
    ],
    relations: [],
  };
}

function makeCoreTableResolver() {
  return {
    getNames: async () => ({
      table: 'enfyra_table',
      column: 'enfyra_column',
      relation: 'enfyra_relation',
    }),
    getTableName: async (key: string) => `enfyra_${key}`,
  };
}

describe('SchemaHealingService Mongo integration', () => {
  let available = false;
  let client: MongoClient | undefined;
  let db: Db | undefined;

  beforeAll(async () => {
    available = await probeMongo();
    if (!available) {
      if (process.env.REQUIRE_MONGO_E2E === '1') {
        throw new Error('Required MongoDB E2E target is unavailable');
      }
      return;
    }

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
      console.warn(
        'MongoDB not available, skipping real DB schema healing test',
      );
      return;
    }

    const columnId = new ObjectId();

    await db.collection('enfyra_column').insertOne({
      _id: columnId,
      tableId: new ObjectId(),
      name: '_id',
      type: 'uuid',
      isPrimary: true,
      isGenerated: true,
      isNullable: false,
    });

    const settingTable = makeTableMetadata('enfyra_setting');
    const columnTable = makeTableMetadata('enfyra_column');
    const tables = new Map<string, any>([
      ['enfyra_setting', settingTable],
      ['enfyra_column', columnTable],
    ]);
    const queryBuilderService = new QueryBuilderService({
      mongoService: {
        getDb: () => db,
        collection: (name: string) => db.collection(name),
        updateOne: async (collectionName: string, id: string, data: any) =>
          db
            .collection(collectionName)
            .updateOne(
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
      reload: async () => undefined,
    };

    const service = new SchemaHealingService({
      queryBuilderService,
      metadataCacheService: metadataCacheService as any,
      systemCoreTableResolver: makeCoreTableResolver() as any,
    });

    await service.runIfNeeded();

    const repairedColumn = await db
      .collection('enfyra_column')
      .findOne({ _id: columnId });

    expect(repairedColumn).toMatchObject({
      _id: columnId,
      name: '_id',
      type: 'objectId',
      isPrimary: true,
    });
  });
});
