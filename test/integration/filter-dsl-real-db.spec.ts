import { SqlQueryExecutor, MongoQueryExecutor } from '@enfyra/kernel';
import { oracleExtensionRowIds } from '../query-builder/filter-reference-extension-oracle';
import { buildIntegrationFilterList } from './filter-dsl-cases';
import { makeMetadata } from './filter-dsl-metadata';
import {
  createIsolatedPostgres,
  createIsolatedMysql,
  createIsolatedMongo,
  PgContext,
  MysqlContext,
  MongoContext,
} from './filter-dsl-db-lifecycle';
import { FilterTestMongoService } from './filter-dsl-mongo-service';

const INTEGRATION = process.env.FILTER_INTEGRATION === '1';

function resolveSteps(): number[] {
  const grow = process.env.FILTER_TEST_GROW_STEPS;
  if (grow && grow.trim()) {
    return grow
      .split(',')
      .map((s) => parseInt(s.trim(), 10))
      .filter((n) => !Number.isNaN(n) && n > 0)
      .map((n) => Math.min(10000, n))
      .sort((a, b) => a - b);
  }
  const max = Math.min(
    10000,
    Math.max(1, parseInt(process.env.FILTER_TEST_MAX_CASES || '2000', 10)),
  );
  return [max];
}

const STEPS = resolveSteps();
const GLOBAL_MAX = Math.min(10000, Math.max(...STEPS));
const ALL_FILTERS = buildIntegrationFilterList(GLOBAL_MAX);
const DEFAULT_PG = 'postgresql://root:1234@127.0.0.1:5432/postgres';
const DEFAULT_MYSQL = 'mysql://root:1234@127.0.0.1:3306/mysql';
const DEFAULT_MONGO =
  'mongodb://enfyra_admin:enfyra_password_123@127.0.0.1:27017/?authSource=admin';

async function sqlRowIds(
  executor: SqlQueryExecutor,
  metadata: ReturnType<typeof makeMetadata>,
  filter: any,
): Promise<number[]> {
  const r = await executor.execute({
    tableName: 'extension',
    filter,
    fields: ['id'],
    sort: 'id',
    metadata,
  });
  return (r.data as any[]).map((x: any) => Number(x.id)).sort((a, b) => a - b);
}

async function mongoRowIds(
  executor: MongoQueryExecutor,
  metadata: ReturnType<typeof makeMetadata>,
  filter: any,
): Promise<number[]> {
  const r = await executor.execute({
    tableName: 'extension',
    filter,
    fields: ['id'],
    sort: 'id',
    metadata,
    dbType: 'mongodb',
  });
  return (r.data as any[]).map((x: any) => Number(x.id)).sort((a, b) => a - b);
}

function registerSqlSuite(
  label: string,
  dbLabel: 'postgres' | 'mysql',
  cap: number,
  ctxFactory: () => Promise<PgContext | MysqlContext>,
) {
  const filters = ALL_FILTERS.slice(0, Math.min(cap, ALL_FILTERS.length));
  const metadata = makeMetadata();

  describe(`${label} (cap ${cap})`, () => {
    let ctx: PgContext | MysqlContext | null = null;

    beforeAll(async () => {
      ctx = await ctxFactory();
    }, 120000);

    afterAll(async () => {
      if (ctx) {
        await ctx.cleanup();
      }
    }, 120000);

    test(`${dbLabel} oracle (batch)`, async () => {
      const executor = new SqlQueryExecutor(ctx!.knex, dbLabel);
      for (const filter of filters) {
        const got = await sqlRowIds(executor, metadata, filter);
        expect(got).toEqual(oracleExtensionRowIds(filter));
      }
    }, 120000);
  });
}

function registerMongoSuite(
  label: string,
  cap: number,
  ctxFactory: () => Promise<MongoContext>,
) {
  const filters = ALL_FILTERS.slice(0, Math.min(cap, ALL_FILTERS.length));
  const metadata = makeMetadata();

  describe(`${label} (cap ${cap})`, () => {
    let ctx: MongoContext | null = null;
    let executor: MongoQueryExecutor;

    beforeAll(async () => {
      ctx = await ctxFactory();
      executor = new MongoQueryExecutor(new FilterTestMongoService(ctx.db));
    }, 120000);

    afterAll(async () => {
      if (ctx) {
        await ctx.cleanup();
      }
    }, 120000);

    test(`mongodb oracle (batch)`, async () => {
      for (const filter of filters) {
        const got = await mongoRowIds(executor, metadata, filter);
        expect(got).toEqual(oracleExtensionRowIds(filter));
      }
    }, 120000);
  });
}

if (!INTEGRATION) {
  describe.skip('filter-dsl-real-db (set FILTER_INTEGRATION=1)', () => {
    it('skipped', () => undefined);
  });
} else {
  jest.setTimeout(120000);

  const pgUrl = process.env.FILTER_TEST_PG_URL || DEFAULT_PG;
  const mysqlUrl = process.env.FILTER_TEST_MYSQL_URL || DEFAULT_MYSQL;
  const mongoUri = process.env.FILTER_TEST_MONGO_URI || DEFAULT_MONGO;
  const skipMysql = process.env.FILTER_TEST_SKIP_MYSQL === '1';

  for (const cap of STEPS) {
    registerSqlSuite('PostgreSQL filter DSL', 'postgres', cap, () =>
      createIsolatedPostgres(pgUrl),
    );

    if (!skipMysql) {
      registerSqlSuite('MySQL filter DSL', 'mysql', cap, () =>
        createIsolatedMysql(mysqlUrl),
      );
    }

    registerMongoSuite('MongoDB filter DSL', cap, () =>
      createIsolatedMongo(mongoUri),
    );
  }

  if (skipMysql) {
    describe.skip('MySQL filter DSL (FILTER_TEST_SKIP_MYSQL=1)', () => {
      it('skipped', () => undefined);
    });
  }
}
