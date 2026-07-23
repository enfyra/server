import knex, { type Knex } from 'knex';
import { type Db, MongoClient } from 'mongodb';
import {
  executeBatchFetches,
  executeMongoBatchFetches,
  type BatchFetchDescriptor,
  type BatchTrace,
} from '@enfyra/kernel';

type ProjectionVariant = 'pk' | 'full';
type FilterVariant = 'none' | 'active' | 'score' | 'combined' | 'empty';
type SortVariant =
  | 'default'
  | 'id-asc'
  | 'id-desc'
  | 'score-asc'
  | 'score-desc'
  | 'score-desc-id-desc';

interface MatrixCase {
  name: string;
  projection: ProjectionVariant;
  filter: FilterVariant;
  sort: SortVariant;
  userLimit?: number;
  userPage?: number;
}

interface TargetRow {
  id: number;
  label: string;
  score: number;
  active: boolean;
}

interface MatrixHarness {
  pkField: 'id' | '_id';
  makeParents(ids: number[]): any[];
  run(
    parents: any[],
    descriptor: BatchFetchDescriptor,
    trace: MatrixTrace,
  ): Promise<void>;
  countQueries(task: () => Promise<void>): Promise<number>;
  measureQueryConcurrency(
    task: () => Promise<void>,
  ): Promise<{ count: number; maxActive: number }>;
}

interface ExpectedCase {
  byParent: Map<number, TargetRow[]>;
  edgeCount: number;
  uniqueTargetCount: number;
}

const PARENT_IDS = [1, 2, 3, 4, 5, 6];
const TARGETS: TargetRow[] = Array.from({ length: 30 }, (_, index) => {
  const id = index + 1;
  return {
    id,
    label: `target-${id}`,
    score: id % 5,
    active: id % 2 === 0,
  };
});
const TARGET_BY_ID = new Map(TARGETS.map((target) => [target.id, target]));
const EDGE_IDS = new Map<number, number[]>([
  [1, Array.from({ length: 15 }, (_, index) => index + 1)],
  [2, [1, 2, 3, ...Array.from({ length: 10 }, (_, index) => index + 16)]],
  [3, []],
  [4, [30, 29, 28, 27]],
  [5, [5, 10, 15, 20, 25, 30]],
  [6, Array.from({ length: 30 }, (_, index) => index + 1)],
]);

const PAGINATION_VARIANTS = [
  { name: 'default-limit' },
  { name: 'unbounded', userLimit: 0 },
  { name: 'limit-1-page-1', userLimit: 1, userPage: 1 },
  { name: 'limit-1-page-2', userLimit: 1, userPage: 2 },
  { name: 'limit-3-page-1', userLimit: 3, userPage: 1 },
  { name: 'limit-3-page-2', userLimit: 3, userPage: 2 },
  { name: 'limit-3-page-99', userLimit: 3, userPage: 99 },
] as const;
const SORT_VARIANTS: SortVariant[] = [
  'default',
  'id-asc',
  'id-desc',
  'score-asc',
  'score-desc',
  'score-desc-id-desc',
];
const FILTER_VARIANTS: FilterVariant[] = [
  'none',
  'active',
  'score',
  'combined',
  'empty',
];
const PROJECTION_VARIANTS: ProjectionVariant[] = ['pk', 'full'];
const MATRIX_CASES: MatrixCase[] = PAGINATION_VARIANTS.flatMap((pagination) =>
  SORT_VARIANTS.flatMap((sort) =>
    FILTER_VARIANTS.flatMap((filter) =>
      PROJECTION_VARIANTS.map((projection) => ({
        name: [pagination.name, sort, filter, projection].join(' | '),
        projection,
        filter,
        sort,
        userLimit: 'userLimit' in pagination ? pagination.userLimit : undefined,
        userPage: 'userPage' in pagination ? pagination.userPage : undefined,
      })),
    ),
  ),
);

class MatrixTrace implements BatchTrace {
  entries: Array<{
    stage: string;
    meta?: Record<string, unknown>;
  }> = [];

  dur(stage: string, _startTs: number, meta?: Record<string, unknown>): number {
    this.entries.push({ stage, meta });
    return 0;
  }

  relation() {
    return this.entries.find((entry) =>
      entry.stage.includes('batch_fetch_L0_targets'),
    );
  }
}

function descriptorFor(
  matrixCase: MatrixCase,
  targetTable: string,
  junctionTable: string,
  sourceColumn: string,
  targetColumn: string,
  pkField: 'id' | '_id',
): BatchFetchDescriptor {
  const filters: Record<FilterVariant, Record<string, unknown> | undefined> = {
    none: undefined,
    active: { active: { _eq: true } },
    score: { score: { _gte: 3 } },
    combined: {
      _and: [{ active: { _eq: true } }, { score: { _gte: 3 } }],
    },
    empty: { score: { _gt: 999 } },
  };
  const sorts: Record<SortVariant, string | string[] | undefined> = {
    default: undefined,
    'id-asc': 'id',
    'id-desc': '-id',
    'score-asc': 'score',
    'score-desc': '-score',
    'score-desc-id-desc': ['-score', '-id'],
  };

  return {
    relationName: 'targets',
    type: 'many-to-many',
    targetTable,
    fields:
      matrixCase.projection === 'pk'
        ? [pkField]
        : [pkField, 'label', 'score', 'active'],
    isInverse: false,
    junctionTableName: junctionTable,
    junctionSourceColumn: sourceColumn,
    junctionTargetColumn: targetColumn,
    userFilter: filters[matrixCase.filter],
    userSort: sorts[matrixCase.sort],
    userLimit: matrixCase.userLimit,
    userPage: matrixCase.userPage,
  };
}

function filterTarget(target: TargetRow, filter: FilterVariant): boolean {
  if (filter === 'active') return target.active;
  if (filter === 'score') return target.score >= 3;
  if (filter === 'combined') return target.active && target.score >= 3;
  if (filter === 'empty') return target.score > 999;
  return true;
}

function compareTargets(
  left: TargetRow,
  right: TargetRow,
  sort: SortVariant,
): number {
  if (sort === 'id-desc') return right.id - left.id;
  if (sort === 'score-asc') {
    return left.score - right.score || left.id - right.id;
  }
  if (sort === 'score-desc') {
    return right.score - left.score || left.id - right.id;
  }
  if (sort === 'score-desc-id-desc') {
    return right.score - left.score || right.id - left.id;
  }
  return left.id - right.id;
}

function expectedCase(
  matrixCase: MatrixCase,
  parentIds: number[] = PARENT_IDS,
): ExpectedCase {
  const byParent = new Map<number, TargetRow[]>();
  const selectedIds = new Set<number>();
  let edgeCount = 0;
  const effectiveLimit =
    matrixCase.userLimit === undefined ? 10 : matrixCase.userLimit;
  const offset =
    effectiveLimit > 0 ? ((matrixCase.userPage ?? 1) - 1) * effectiveLimit : 0;

  for (const parentId of new Set(parentIds)) {
    const targets = (EDGE_IDS.get(parentId) ?? [])
      .map((targetId) => TARGET_BY_ID.get(targetId)!)
      .filter((target) => filterTarget(target, matrixCase.filter))
      .sort((left, right) => compareTargets(left, right, matrixCase.sort));
    const selected =
      effectiveLimit > 0
        ? targets.slice(offset, offset + effectiveLimit)
        : targets;
    byParent.set(parentId, selected);
    edgeCount += selected.length;
    for (const target of selected) selectedIds.add(target.id);
  }

  return {
    byParent,
    edgeCount,
    uniqueTargetCount: selectedIds.size,
  };
}

function normalizeTarget(
  target: any,
  pkField: 'id' | '_id',
  projection: ProjectionVariant,
): Record<string, unknown> {
  if (projection === 'pk') {
    return { id: target[pkField] };
  }
  return {
    id: target[pkField],
    label: target.label,
    score: target.score,
    active: Boolean(target.active),
  };
}

function expectedTarget(
  target: TargetRow,
  projection: ProjectionVariant,
): Record<string, unknown> {
  if (projection === 'pk') return { id: target.id };
  return { ...target };
}

function assertResult(
  parents: any[],
  harness: MatrixHarness,
  matrixCase: MatrixCase,
  expected: ExpectedCase,
): void {
  const refsByTarget = new Map<number, any>();
  const expectedKeys =
    matrixCase.projection === 'pk'
      ? [harness.pkField]
      : [harness.pkField, 'active', 'label', 'score'].sort();

  for (const parent of parents) {
    const parentId = parent[harness.pkField];
    const actualTargets = parent.targets;
    expect(
      actualTargets.map((target: any) =>
        normalizeTarget(target, harness.pkField, matrixCase.projection),
      ),
    ).toStrictEqual(
      (expected.byParent.get(parentId) ?? []).map((target) =>
        expectedTarget(target, matrixCase.projection),
      ),
    );
    for (const target of actualTargets) {
      expect(Object.keys(target).sort()).toStrictEqual(expectedKeys);
      const targetId = target[harness.pkField];
      const previous = refsByTarget.get(targetId);
      if (previous) {
        expect(target).toBe(previous);
      } else {
        refsByTarget.set(targetId, target);
      }
    }
  }
}

function expectedStrategy(matrixCase: MatrixCase): string {
  const limited =
    matrixCase.userLimit === undefined || matrixCase.userLimit > 0;
  if (matrixCase.projection === 'full') {
    return limited ? 'm2m-partitioned-edge-loader' : 'm2m-edge-loader';
  }
  const junctionOnly =
    matrixCase.filter === 'none' &&
    ['default', 'id-asc', 'id-desc'].includes(matrixCase.sort);
  if (junctionOnly) {
    return limited ? 'm2m-junction-partitioned-top-k' : 'm2m-junction-batch';
  }
  return limited ? 'm2m-filtered-edge-top-k' : 'm2m-filtered-edge-batch';
}

async function executeMatrixCase(
  harness: MatrixHarness,
  matrixCase: MatrixCase,
  descriptor: BatchFetchDescriptor,
): Promise<void> {
  const parents = harness.makeParents(PARENT_IDS);
  const trace = new MatrixTrace();
  const expected = expectedCase(matrixCase);
  const queryCount = await harness.countQueries(() =>
    harness.run(parents, descriptor, trace),
  );
  const expectedQueries =
    matrixCase.projection === 'pk' || expected.uniqueTargetCount === 0 ? 1 : 2;
  const relationTrace = trace.relation();

  assertResult(parents, harness, matrixCase, expected);
  expect(queryCount).toBe(expectedQueries);
  expect(relationTrace).toBeDefined();
  expect(relationTrace!.meta).toMatchObject({
    strategy: expectedStrategy(matrixCase),
    roundtrips: expectedQueries,
    ioConcurrency: 2,
    rowsTransferred:
      expected.edgeCount +
      (matrixCase.projection === 'full' ? expected.uniqueTargetCount : 0),
    rowsReturned: expected.edgeCount,
    rowsDiscarded: 0,
    userLimit: matrixCase.userLimit === undefined ? 10 : matrixCase.userLimit,
    userFilter: matrixCase.filter !== 'none',
    userSort: matrixCase.sort !== 'default',
  });
}

function registerInvariantTests(
  harness: () => MatrixHarness,
  descriptor: () => BatchFetchDescriptor,
): void {
  test('deduplicates parent ids and shares relation arrays and target objects', async () => {
    const activeHarness = harness();
    const parents = activeHarness.makeParents([1, 1, 2]);
    const trace = new MatrixTrace();
    const queries = await activeHarness.countQueries(() =>
      activeHarness.run(parents, descriptor(), trace),
    );

    expect(queries).toBe(2);
    expect(parents[0].targets).toBe(parents[1].targets);
    expect(parents[0].targets[0]).toBe(parents[2].targets[0]);
    expect(trace.relation()!.meta).toMatchObject({
      roundtrips: 2,
      rowsReturned: 2,
      rowsTransferred: 3,
      rowsDiscarded: 0,
    });
  });

  test('does no metadata lookup or database query for empty parent input', async () => {
    const activeHarness = harness();
    const trace = new MatrixTrace();
    const queries = await activeHarness.countQueries(() =>
      activeHarness.run([], descriptor(), trace),
    );

    expect(queries).toBe(0);
    expect(trace.entries).toStrictEqual([]);
  });

  test('chunks 5001 parent ids into two edge queries and one target query', async () => {
    const activeHarness = harness();
    const parents = activeHarness.makeParents(
      Array.from({ length: 5001 }, (_, index) => index + 1),
    );
    const trace = new MatrixTrace();
    const metrics = await activeHarness.measureQueryConcurrency(() =>
      activeHarness.run(parents, descriptor(), trace),
    );

    expect(metrics.count).toBe(3);
    expect(metrics.maxActive).toBeGreaterThanOrEqual(1);
    expect(metrics.maxActive).toBeLessThanOrEqual(2);
    expect(parents[2].targets).toStrictEqual([]);
    expect(parents[5000].targets).toStrictEqual([]);
    expect(trace.relation()!.meta).toMatchObject({
      strategy: 'm2m-partitioned-edge-loader',
      roundtrips: 3,
      ioConcurrency: 2,
      rowsTransferred: 8,
      rowsReturned: 5,
      rowsDiscarded: 0,
    });
  });

  test('rejects an unknown local sort before mutating parent documents', async () => {
    const activeHarness = harness();
    const parents = activeHarness.makeParents([1, 2]);
    const trace = new MatrixTrace();
    const invalidDescriptor = {
      ...descriptor(),
      userSort: 'missingField',
    };

    await expect(
      activeHarness.run(parents, invalidDescriptor, trace),
    ).rejects.toThrow("Sort field 'missingField' does not exist");
    expect(parents.every((parent) => !('targets' in parent))).toBe(true);
    expect(trace.entries).toStrictEqual([]);
  });

  test('rejects missing target metadata without partial assignment', async () => {
    const activeHarness = harness();
    const parents = activeHarness.makeParents([1, 2]);
    const trace = new MatrixTrace();
    const invalidDescriptor = {
      ...descriptor(),
      targetTable: '__missing_m2m_matrix_target__',
    };

    await expect(
      activeHarness.run(parents, invalidDescriptor, trace),
    ).rejects.toThrow('Metadata not found for target table');
    expect(parents.every((parent) => !('targets' in parent))).toBe(true);
    expect(trace.entries).toStrictEqual([]);
  });
}

const SQL_CONFIGS = [
  {
    name: 'postgres',
    client: 'pg',
    connection:
      process.env.PG_TEST_URI ||
      'postgresql://root:1234@localhost:5432/postgres',
    dbType: 'postgres' as const,
  },
  {
    name: 'mysql',
    client: 'mysql2',
    connection:
      process.env.MYSQL_TEST_URI || 'mysql://root:1234@localhost:3306/enfyra',
    dbType: 'mysql' as const,
  },
];

for (const config of SQL_CONFIGS) {
  describe.sequential(`m2m loader matrix (${config.name})`, () => {
    const prefix = `__m2m_mx_${Date.now()}_${config.name}_`;
    const tables = {
      parents: `${prefix}parents`,
      targets: `${prefix}targets`,
      junction: `${prefix}junction`,
    };
    const metadata: Record<string, any> = {
      [tables.parents]: {
        name: tables.parents,
        columns: [{ name: 'id', type: 'integer', isPrimary: true }],
        relations: [
          {
            propertyName: 'targets',
            type: 'many-to-many',
            targetTableName: tables.targets,
            targetTable: tables.targets,
            isInverse: false,
            junctionTableName: tables.junction,
            junctionSourceColumn: 'parentId',
            junctionTargetColumn: 'targetId',
          },
        ],
      },
      [tables.targets]: {
        name: tables.targets,
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'label', type: 'varchar' },
          { name: 'score', type: 'integer' },
          { name: 'active', type: 'boolean' },
        ],
        relations: [],
      },
    };
    let db: Knex;
    let available = true;
    let harness: MatrixHarness;

    beforeAll(async () => {
      db = knex({
        client: config.client,
        connection: config.connection,
        pool: { min: 0, max: 4 },
      });
      try {
        await db.raw('SELECT 1');
      } catch {
        available = false;
        return;
      }

      await db.schema.dropTableIfExists(tables.junction);
      await db.schema.dropTableIfExists(tables.targets);
      await db.schema.dropTableIfExists(tables.parents);
      await db.schema.createTable(tables.parents, (table) => {
        table.integer('id').primary();
      });
      await db.schema.createTable(tables.targets, (table) => {
        table.integer('id').primary();
        table.string('label').notNullable();
        table.integer('score').notNullable();
        table.boolean('active').notNullable();
      });
      await db.schema.createTable(tables.junction, (table) => {
        table.integer('parentId').notNullable().index();
        table.integer('targetId').notNullable().index();
        table.unique(['parentId', 'targetId']);
      });
      await db(tables.parents).insert(PARENT_IDS.map((id) => ({ id })));
      await db(tables.targets).insert(TARGETS);
      await db(tables.junction).insert(
        Array.from(EDGE_IDS.entries()).flatMap(([parentId, targetIds]) =>
          targetIds.map((targetId) => ({ parentId, targetId })),
        ),
      );

      harness = {
        pkField: 'id',
        makeParents: (ids) => ids.map((id) => ({ id })),
        run: async (parents, descriptor, trace) => {
          await executeBatchFetches(
            db,
            parents,
            [descriptor],
            async (table) => metadata[table] ?? null,
            3,
            0,
            tables.parents,
            config.dbType,
            metadata,
            trace,
          );
        },
        countQueries: async (task) => {
          let count = 0;
          const listener = () => {
            count += 1;
          };
          db.on('query', listener);
          try {
            await task();
          } finally {
            db.removeListener('query', listener);
          }
          return count;
        },
        measureQueryConcurrency: async (task) => {
          let count = 0;
          let maxActive = 0;
          const active = new Set<string>();
          const onQuery = (query: { __knexQueryUid?: string }) => {
            const id = query.__knexQueryUid ?? `query-${count}`;
            count += 1;
            active.add(id);
            maxActive = Math.max(maxActive, active.size);
          };
          const onComplete = (
            _result: unknown,
            query: { __knexQueryUid?: string },
          ) => {
            if (query?.__knexQueryUid) active.delete(query.__knexQueryUid);
          };
          const onError = (
            _error: unknown,
            query: { __knexQueryUid?: string },
          ) => {
            if (query?.__knexQueryUid) active.delete(query.__knexQueryUid);
          };
          db.on('query', onQuery);
          db.on('query-response', onComplete);
          db.on('query-error', onError);
          try {
            await task();
          } finally {
            db.removeListener('query', onQuery);
            db.removeListener('query-response', onComplete);
            db.removeListener('query-error', onError);
          }
          return { count, maxActive };
        },
      };
    }, 30_000);

    afterAll(async () => {
      if (!db) return;
      if (available) {
        await db.schema.dropTableIfExists(tables.junction);
        await db.schema.dropTableIfExists(tables.targets);
        await db.schema.dropTableIfExists(tables.parents);
      }
      await db.destroy();
    }, 30_000);

    test.each(MATRIX_CASES)('$name', async (matrixCase) => {
      if (!available) return;
      await executeMatrixCase(
        harness,
        matrixCase,
        descriptorFor(
          matrixCase,
          tables.targets,
          tables.junction,
          'parentId',
          'targetId',
          'id',
        ),
      );
    });

    registerInvariantTests(
      () => harness,
      () =>
        descriptorFor(
          {
            name: 'invariant',
            projection: 'full',
            filter: 'none',
            sort: 'id-asc',
            userLimit: 1,
            userPage: 1,
          },
          tables.targets,
          tables.junction,
          'parentId',
          'targetId',
          'id',
        ),
    );
  });
}

describe.sequential('m2m loader matrix (mongodb)', () => {
  const databaseName = `m2m_matrix_${Date.now()}`;
  const collections = {
    parents: 'parents',
    targets: 'targets',
    junction: 'parents_targets',
  };
  const metadata: Record<string, any> = {
    [collections.parents]: {
      name: collections.parents,
      columns: [{ name: '_id', type: 'integer', isPrimary: true }],
      relations: [
        {
          propertyName: 'targets',
          type: 'many-to-many',
          targetTableName: collections.targets,
          targetTable: collections.targets,
          isInverse: false,
          junctionTableName: collections.junction,
          junctionSourceColumn: 'sourceId',
          junctionTargetColumn: 'targetId',
        },
      ],
    },
    [collections.targets]: {
      name: collections.targets,
      columns: [
        { name: '_id', type: 'integer', isPrimary: true },
        { name: 'label', type: 'varchar' },
        { name: 'score', type: 'integer' },
        { name: 'active', type: 'boolean' },
      ],
      relations: [],
    },
  };
  let client: MongoClient;
  let db: Db;
  let available = true;
  let harness: MatrixHarness;

  beforeAll(async () => {
    client = new MongoClient(
      process.env.MONGO_TEST_URI ||
        'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin',
      { monitorCommands: true },
    );
    try {
      await client.connect();
    } catch {
      available = false;
      return;
    }
    db = client.db(databaseName);
    await db
      .collection(collections.parents)
      .insertMany(PARENT_IDS.map((id) => ({ _id: id })));
    await db
      .collection(collections.targets)
      .insertMany(TARGETS.map(({ id, ...target }) => ({ _id: id, ...target })));
    await db
      .collection(collections.junction)
      .insertMany(
        Array.from(EDGE_IDS.entries()).flatMap(([sourceId, targetIds]) =>
          targetIds.map((targetId) => ({ sourceId, targetId })),
        ),
      );
    await db
      .collection(collections.junction)
      .createIndex({ sourceId: 1, targetId: 1 }, { unique: true });

    harness = {
      pkField: '_id',
      makeParents: (ids) => ids.map((_id) => ({ _id })),
      run: async (parents, descriptor, trace) => {
        await executeMongoBatchFetches(
          db,
          parents,
          [descriptor],
          async (table) => metadata[table] ?? null,
          3,
          0,
          collections.parents,
          { tables: new Map(Object.entries(metadata)) },
          trace,
        );
      },
      countQueries: async (task) => {
        let count = 0;
        const listener = (event: {
          databaseName: string;
          commandName: string;
          command: Record<string, unknown>;
        }) => {
          if (event.databaseName !== databaseName) return;
          const collection = event.command[event.commandName];
          if (
            ['find', 'aggregate'].includes(event.commandName) &&
            [collections.junction, collections.targets].includes(
              String(collection),
            )
          ) {
            count += 1;
          }
        };
        client.on('commandStarted', listener);
        try {
          await task();
        } finally {
          client.removeListener('commandStarted', listener);
        }
        return count;
      },
      measureQueryConcurrency: async (task) => {
        let count = 0;
        let maxActive = 0;
        const active = new Set<number>();
        const onStarted = (event: {
          requestId: number;
          databaseName: string;
          commandName: string;
          command: Record<string, unknown>;
        }) => {
          if (event.databaseName !== databaseName) return;
          const collection = event.command[event.commandName];
          if (
            !['find', 'aggregate'].includes(event.commandName) ||
            ![collections.junction, collections.targets].includes(
              String(collection),
            )
          ) {
            return;
          }
          count += 1;
          active.add(event.requestId);
          maxActive = Math.max(maxActive, active.size);
        };
        const onComplete = (event: { requestId: number }) => {
          active.delete(event.requestId);
        };
        client.on('commandStarted', onStarted);
        client.on('commandSucceeded', onComplete);
        client.on('commandFailed', onComplete);
        try {
          await task();
        } finally {
          client.removeListener('commandStarted', onStarted);
          client.removeListener('commandSucceeded', onComplete);
          client.removeListener('commandFailed', onComplete);
        }
        return { count, maxActive };
      },
    };
  }, 30_000);

  afterAll(async () => {
    if (available && db) await db.dropDatabase();
    if (client) await client.close();
  }, 30_000);

  test.each(MATRIX_CASES)('$name', async (matrixCase) => {
    if (!available) return;
    await executeMatrixCase(
      harness,
      matrixCase,
      descriptorFor(
        matrixCase,
        collections.targets,
        collections.junction,
        'sourceId',
        'targetId',
        '_id',
      ),
    );
  });

  registerInvariantTests(
    () => harness,
    () =>
      descriptorFor(
        {
          name: 'invariant',
          projection: 'full',
          filter: 'none',
          sort: 'id-asc',
          userLimit: 1,
          userPage: 1,
        },
        collections.targets,
        collections.junction,
        'sourceId',
        'targetId',
        '_id',
      ),
  );
});
