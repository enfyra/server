/**
 * Metadata Reload E2E (loads dist/src MetadataCacheService)
 * Constructor: QueryBuilderService, DatabaseSchemaService, websocketGateway
 *
 * Run: yarn build && node test/metadata-reload.e2e.js
 */

const path = require('path');

let testsPassed = 0;
let testsFailed = 0;

function log(name, passed, details = '') {
  const status = passed ? '✓ PASS' : '✗ FAIL';
  console.log(`${status}: ${name}${details ? '\n         ' + details : ''}`);
  if (passed) testsPassed++;
  else testsFailed++;
}

async function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

class MockQueryBuilder {
  constructor(loadDelay = 100) {
    this.loadDelay = loadDelay;
    this.queryCount = 0;
  }

  async select({ tableName }) {
    this.queryCount++;
    if (tableName === 'table_definition') {
      await sleep(this.loadDelay);
      return {
        data: [
          { id: 1, name: 'users', alias: 'users' },
          { id: 2, name: 'posts', alias: 'posts' },
        ],
      };
    }
    if (tableName === 'column_definition') {
      return { data: [] };
    }
    if (tableName === 'relation_definition') {
      return { data: [] };
    }
    return { data: [] };
  }

  isMongoDb() {
    return false;
  }

  getQueryCount() {
    return this.queryCount;
  }

  reset() {
    this.queryCount = 0;
  }
}

class MockDatabaseSchemaService {
  async getAllTableSchemas() {
    const m = new Map();
    m.set('users', { columns: [], relations: [], uniques: [], indexes: [] });
    m.set('posts', { columns: [], relations: [], uniques: [], indexes: [] });
    return m;
  }

  async getTableSchemas(tableNames) {
    const m = new Map();
    for (const n of tableNames) {
      m.set(n, { columns: [], relations: [], uniques: [], indexes: [] });
    }
    return m;
  }
}

function createWsRecorder() {
  const emitted = [];
  return {
    emitted,
    gateway: {
      emitToNamespace: (_ns, _event, data) => {
        emitted.push(data);
      },
    },
  };
}

async function runTests() {
  console.log('='.repeat(70));
  console.log('METADATA RELOAD E2E (MetadataCacheService)');
  console.log('='.repeat(70));

  const distPath = path.join(__dirname, '..', 'dist', 'src');
  const { MetadataCacheService } = require(
    path.join(distPath, 'infrastructure', 'cache', 'services', 'metadata-cache.service'),
  );

  const ws1 = createWsRecorder();
  const queryBuilder1 = new MockQueryBuilder(50);
  const dbSchema1 = new MockDatabaseSchemaService();

  const service1 = new MetadataCacheService(
    queryBuilder1,
    dbSchema1,
    ws1.gateway,
  );

  console.log('\n--- TEST 1: Single instance reload timing ---');
  const reloadStart1 = Date.now();
  await service1.reload();
  const reloadDuration1 = Date.now() - reloadStart1;

  log(
    'Reload completes in expected time',
    reloadDuration1 >= 40 && reloadDuration1 < 400,
    `duration: ${reloadDuration1}ms`,
  );

  const metadata1 = await service1.getMetadata();
  log(
    'Metadata available after reload',
    metadata1 !== null && metadata1.tablesList.length > 0,
    `tables: ${metadata1.tablesList.length}`,
  );

  const pendingDone =
    ws1.emitted.some((d) => d && d.status === 'pending') &&
    ws1.emitted.some((d) => d && d.status === 'done');
  log('WebSocket emits pending then done during reload', pendingDone, `events: ${ws1.emitted.length}`);

  console.log('\n--- TEST 2: Concurrent reloads deduplicate ---');
  queryBuilder1.reset();
  const reloadPromises = [];
  const t0 = Date.now();
  for (let i = 0; i < 5; i++) {
    reloadPromises.push(service1.reload());
  }
  await Promise.all(reloadPromises);
  const span = Date.now() - t0;

  log('All concurrent reloads complete', true, `${reloadPromises.length} calls`);
  log(
    'Single load path (not 5x DB work)',
    queryBuilder1.getQueryCount() <= 6,
    `select calls: ${queryBuilder1.getQueryCount()} (expect ~3 for one load)`,
  );
  log('Total wall time reflects deduplication', span < 250, `span: ${span}ms`);

  console.log('\n--- TEST 3: Independent instances each load metadata ---');
  const queryBuilderA = new MockQueryBuilder(80);
  const queryBuilderB = new MockQueryBuilder(80);
  const queryBuilderC = new MockQueryBuilder(80);
  const dbSchema = new MockDatabaseSchemaService();

  const serviceA = new MetadataCacheService(queryBuilderA, dbSchema, createWsRecorder().gateway);
  const serviceB = new MetadataCacheService(queryBuilderB, dbSchema, createWsRecorder().gateway);
  const serviceC = new MetadataCacheService(queryBuilderC, dbSchema, createWsRecorder().gateway);

  await Promise.all([serviceA.reload(), serviceB.reload(), serviceC.reload()]);

  const metaA = await serviceA.getMetadata();
  const metaB = await serviceB.getMetadata();
  const metaC = await serviceC.getMetadata();
  const totalQueries =
    queryBuilderA.getQueryCount() + queryBuilderB.getQueryCount() + queryBuilderC.getQueryCount();

  log('Each instance has metadata', metaA && metaB && metaC, '');
  log(
    'Each instance queried DB (3 selects per reload)',
    totalQueries >= 9,
    `total select calls: ${totalQueries}`,
  );

  console.log('\n--- TEST 4: getMetadata during reload keeps old version until done ---');
  const ws5 = createWsRecorder();
  const queryBuilder5 = new MockQueryBuilder(120);
  const service5 = new MetadataCacheService(queryBuilder5, new MockDatabaseSchemaService(), ws5.gateway);

  await service5.reload();
  const oldMetadata = await service5.getMetadata();
  const oldVersion = oldMetadata.version;

  queryBuilder5.loadDelay = 180;
  const reloadPromise5 = service5.reload();
  await sleep(25);

  const getMetadataStart5 = Date.now();
  const metadata5During = await service5.getMetadata();
  const getMetadataDuration5 = Date.now() - getMetadataStart5;

  await reloadPromise5;
  const metadata5After = await service5.getMetadata();

  log(
    'getMetadata returns quickly while reload runs (cached)',
    getMetadataDuration5 < 100,
    `wait: ${getMetadataDuration5}ms`,
  );
  log(
    'Version bumps only after reload completes',
    metadata5During.version === oldVersion && metadata5After.version > oldVersion,
    `old: ${oldVersion}, after: ${metadata5After.version}`,
  );

  console.log('\n--- TEST 5: Reload does not double-query without second reload ---');
  const queryBuilder7 = new MockQueryBuilder(40);
  const service7 = new MetadataCacheService(queryBuilder7, new MockDatabaseSchemaService(), createWsRecorder().gateway);
  await service7.reload();
  const qAfter = queryBuilder7.getQueryCount();
  await sleep(80);
  log(
    'No extra DB work after idle sleep',
    queryBuilder7.getQueryCount() === qAfter,
    `queries: ${qAfter}`,
  );

  console.log('\n' + '='.repeat(70));
  console.log(`RESULTS: ${testsPassed} passed, ${testsFailed} failed`);
  console.log('='.repeat(70));

  process.exit(testsFailed > 0 ? 1 : 0);
}

runTests().catch((e) => {
  console.error('Test Error:', e);
  process.exit(1);
});
