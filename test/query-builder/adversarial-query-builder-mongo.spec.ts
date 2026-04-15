import { MongoClient, Db, ObjectId } from 'mongodb';
import { MongoQueryExecutor } from 'src/infrastructure/query-builder/executors/mongo-query-executor';
import { QueryPlanner } from 'src/infrastructure/query-builder/planner/query-planner';

const MONGO_URI =
  process.env.MONGO_TEST_URI ||
  'mongodb://enfyra_admin:enfyra_password_123@localhost:27017/?authSource=admin';
const DB_NAME = `test_adversarial_mongo_${Date.now()}`;

type RelStub = {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  targetTableName: string;
  foreignKeyColumn?: string;
  mappedBy?: string;
  isInverse?: boolean;
};

function makeTableMeta(
  name: string,
  columnNames: string[],
  relations: RelStub[],
) {
  return {
    id: 1,
    name,
    isSystem: false,
    columns: columnNames.map((n, i) => ({
      id: i + 1,
      name: n,
      type: 'mixed',
      isPrimary: n === '_id',
      isGenerated: n === '_id',
      isNullable: true,
      isSystem: false,
      isUpdatable: true,
      tableId: 1,
    })),
    relations: relations as any[],
  };
}

async function probeMongo(): Promise<boolean> {
  try {
    const c = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 2000 });
    await c.connect();
    await c.close();
    return true;
  } catch {
    return false;
  }
}

describe('Adversarial Query Builder (MongoQueryExecutor parity)', () => {
  let available = false;
  let client: MongoClient;
  let db: Db;
  let executor: MongoQueryExecutor;
  let meta: any;

  const menuIds: ObjectId[] = [];
  const userIds: ObjectId[] = [];
  const extIds: ObjectId[] = [];
  const tagIds: ObjectId[] = [];
  const catIds: ObjectId[] = [];
  const chunkParentIds: ObjectId[] = [];
  const warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});

  beforeAll(async () => {
    available = await probeMongo();
    if (!available) return;

    client = new MongoClient(MONGO_URI);
    await client.connect();
    db = client.db(DB_NAME);

    for (let i = 0; i < 4; i++) menuIds.push(new ObjectId());
    for (let i = 0; i < 3; i++) userIds.push(new ObjectId());
    for (let i = 0; i < 7; i++) extIds.push(new ObjectId());
    for (let i = 0; i < 2; i++) tagIds.push(new ObjectId());
    for (let i = 0; i < 3; i++) catIds.push(new ObjectId());

    await db.collection('menu').insertMany([
      { _id: menuIds[0], label: 'm1' },
      { _id: menuIds[1], label: 'm88' },
      { _id: menuIds[2], label: 'm99' },
      { _id: menuIds[3], label: "o'reilly" },
    ]);
    await db.collection('user').insertMany([
      { _id: userIds[0], name: 'alice' },
      { _id: userIds[1], name: 'bob' },
      { _id: userIds[2], name: 'carol' },
    ]);
    await db.collection('extension').insertMany([
      { _id: extIds[0], title: 'alpha', prio: 10, menu: menuIds[1], owner: userIds[0] },
      { _id: extIds[1], title: 'beta', prio: 20, menu: menuIds[1], owner: userIds[1] },
      { _id: extIds[2], title: 'gamma_chunk', prio: 5, menu: menuIds[2], owner: userIds[0] },
      { _id: extIds[3], title: 'delta', prio: 0, menu: null, owner: null },
      { _id: extIds[4], title: 'unicode_你好', prio: 7, menu: menuIds[3], owner: userIds[2] },
      { _id: extIds[5], title: 'Résumé', prio: 8, menu: menuIds[1], owner: userIds[1] },
      { _id: extIds[6], title: '100%_done_literal', prio: 1, menu: menuIds[1], owner: userIds[0] },
    ]);
    await db.collection('ext_note').insertMany([
      { extensionId: extIds[0], body: 'spam' },
      { extensionId: extIds[0], body: 'ok' },
      { extensionId: extIds[0], body: 'dup' },
      { extensionId: extIds[1], body: 'spam' },
    ]);
    await db.collection('ext_tag').insertMany([
      { _id: tagIds[0], label: 'x' },
      { _id: tagIds[1], label: 'y' },
    ]);
    await db.collection('extension_tags_ext_tag').insertMany([
      { extensionId: extIds[0], ext_tagId: tagIds[0] },
      { extensionId: extIds[0], ext_tagId: tagIds[1] },
      { extensionId: extIds[2], ext_tagId: tagIds[0] },
    ]);
    await db.collection('category').insertMany([
      { _id: catIds[0], name: 'root', parent: null },
      { _id: catIds[1], name: 'child', parent: catIds[0] },
      { _id: catIds[2], name: 'grandchild', parent: catIds[1] },
    ]);

    for (let i = 0; i < 6000; i++) chunkParentIds.push(new ObjectId());
    const chunkParentDocs = chunkParentIds.map((id) => ({ _id: id }));
    const chunkChildDocs = chunkParentIds.map((id) => ({
      _id: new ObjectId(),
      parent: id,
    }));
    for (let i = 0; i < chunkParentDocs.length; i += 1000) {
      await db
        .collection('chunk_parent')
        .insertMany(chunkParentDocs.slice(i, i + 1000));
    }
    for (let i = 0; i < chunkChildDocs.length; i += 1000) {
      await db
        .collection('chunk_child')
        .insertMany(chunkChildDocs.slice(i, i + 1000));
    }

    const menuTable = makeTableMeta('menu', ['_id', 'label'], []);
    const userTable = makeTableMeta('user', ['_id', 'name'], []);
    const extNoteTable = makeTableMeta(
      'ext_note',
      ['_id', 'extensionId', 'body'],
      [],
    );
    const extTagTable = makeTableMeta('ext_tag', ['_id', 'label'], []);
    const categoryTable = makeTableMeta(
      'category',
      ['_id', 'name', 'parent'],
      [
        {
          propertyName: 'parent',
          type: 'many-to-one',
          targetTable: 'category',
          targetTableName: 'category',
          foreignKeyColumn: 'parent',
        },
      ],
    );
    const extTable = makeTableMeta(
      'extension',
      ['_id', 'title', 'prio', 'menu', 'owner', 'tags'],
      [
        {
          propertyName: 'menu',
          type: 'many-to-one',
          targetTable: 'menu',
          targetTableName: 'menu',
          foreignKeyColumn: 'menu',
        },
        {
          propertyName: 'owner',
          type: 'many-to-one',
          targetTable: 'user',
          targetTableName: 'user',
          foreignKeyColumn: 'owner',
        },
        {
          propertyName: 'notes',
          type: 'one-to-many',
          targetTable: 'ext_note',
          targetTableName: 'ext_note',
          foreignKeyColumn: 'extensionId',
          mappedBy: 'extensionId',
          isInverse: true,
        },
        {
          propertyName: 'tags',
          type: 'many-to-many',
          targetTable: 'ext_tag',
          targetTableName: 'ext_tag',
        },
      ],
    );

    const chunkChildTable = makeTableMeta(
      'chunk_child',
      ['_id', 'parent'],
      [],
    );
    const chunkParentTable = makeTableMeta(
      'chunk_parent',
      ['_id'],
      [
        {
          propertyName: 'children',
          type: 'one-to-many',
          targetTable: 'chunk_child',
          targetTableName: 'chunk_child',
          foreignKeyColumn: 'parent',
          mappedBy: 'parent',
          isInverse: true,
        },
      ],
    );

    const m = new Map<string, any>();
    m.set('extension', extTable);
    m.set('menu', menuTable);
    m.set('user', userTable);
    m.set('ext_note', extNoteTable);
    m.set('ext_tag', extTagTable);
    m.set('category', categoryTable);
    m.set('chunk_parent', chunkParentTable);
    m.set('chunk_child', chunkChildTable);
    meta = { tables: m };

    const mongoService: any = {
      getDb: () => db,
      collection: (name: string) => db.collection(name),
    };
    executor = new MongoQueryExecutor(mongoService);
  });

  afterAll(async () => {
    warnSpy.mockRestore();
    if (!available) return;
    await db.dropDatabase();
    await client.close();
  });

  function runOrSkip(name: string, fn: () => Promise<void>) {
    test(name, async () => {
      if (!available) {
        console.warn('MongoDB not available, skipping');
        return;
      }
      await fn();
    });
  }

  async function runPlan(options: {
    tableName: string;
    filter?: any;
    fields?: string[];
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
  }) {
    const planner = new QueryPlanner();
    const plan = planner.plan({
      tableName: options.tableName,
      fields: options.fields,
      filter: options.filter,
      sort: options.sort,
      page: options.page,
      limit: options.limit,
      meta: options.meta,
      metadata: meta,
      dbType: 'mongodb' as any,
    });
    return executor.execute({
      tableName: options.tableName,
      filter: options.filter,
      fields: options.fields,
      sort: options.sort,
      page: options.page,
      limit: options.limit,
      meta: options.meta,
      metadata: meta,
      dbType: 'mongodb',
      plan,
    });
  }

  async function idsOf(filter: any, extra: any = {}): Promise<string[]> {
    const r = await runPlan({
      tableName: 'extension',
      filter,
      fields: ['_id'],
      sort: '_id',
      ...extra,
    });
    return (r.data as any[])
      .map((x) => String(x._id))
      .sort();
  }

  function idSetOf(indices: number[]): string[] {
    return indices.map((i) => String(extIds[i])).sort();
  }

  runOrSkip('deep nested _and / _or / _not selects expected rows', async () => {
    const ids = await idsOf({
      _and: [
        { _or: [{ title: { _eq: 'alpha' } }, { title: { _eq: 'delta' } }] },
        { _not: { _not: { _not: { title: { _eq: 'beta' } } } } },
      ],
    });
    expect(ids).toEqual(idSetOf([0, 3]));
  });

  runOrSkip('_and: [] matches all rows', async () => {
    const ids = await idsOf({ _and: [] });
    expect(ids).toEqual(idSetOf([0, 1, 2, 3, 4, 5, 6]));
  });

  runOrSkip('_or single clause matches same as flat', async () => {
    const a = await idsOf({ _or: [{ title: { _eq: 'alpha' } }] });
    const b = await idsOf({ title: { _eq: 'alpha' } });
    expect(a).toEqual(b);
  });

  runOrSkip('_not: {} does not constrain', async () => {
    const ids = await idsOf({ _not: {} });
    expect(ids).toEqual(idSetOf([0, 1, 2, 3, 4, 5, 6]));
  });

  runOrSkip('scalar _is_null on plain column (prio) works on Mongo', async () => {
    const ids = await idsOf({ prio: { _eq: 0 } });
    expect(ids).toEqual(idSetOf([3]));
  });

  runOrSkip('relation-level _is_null / _eq null now works (parity with SQL)', async () => {
    const isNull = await idsOf({ menu: { _is_null: true } });
    const eqNull = await idsOf({ menu: { _eq: null } });
    expect(isNull).toEqual(idSetOf([3]));
    expect(eqNull).toEqual(idSetOf([3]));
  });

  runOrSkip('relation-level _is_not_null excludes rows with null FK', async () => {
    const ids = await idsOf({ menu: { _is_not_null: true } });
    expect(ids).toEqual(idSetOf([0, 1, 2, 4, 5, 6]));
  });

  runOrSkip('_in empty array returns no rows', async () => {
    expect(await idsOf({ _id: { _in: [] } })).toEqual([]);
  });

  runOrSkip('_in with null + real ids returns only real matches', async () => {
    const ids = await idsOf({ _id: { _in: [null, extIds[0], extIds[1]] } });
    expect(ids).toEqual(idSetOf([0, 1]));
  });

  runOrSkip('mutually exclusive _and returns empty', async () => {
    expect(
      await idsOf({
        _and: [{ title: { _eq: 'alpha' } }, { title: { _eq: 'beta' } }],
      }),
    ).toEqual([]);
  });

  runOrSkip('unknown operator throws BadRequest listing supported operators', async () => {
    await expect(idsOf({ title: { _weird: 1 } } as any)).rejects.toMatchObject({
      message: expect.stringContaining('Unsupported filter operator "_weird"'),
    });
    await expect(idsOf({ title: { _weird: 1 } } as any)).rejects.toMatchObject({
      message: expect.stringContaining('_is_null'),
    });
  });

  runOrSkip('_eq preserves literal % and _ in title', async () => {
    expect(await idsOf({ title: { _eq: '100%_done_literal' } })).toEqual(
      idSetOf([6]),
    );
  });

  runOrSkip('_contains matches stable substring', async () => {
    expect(await idsOf({ title: { _contains: 'done_literal' } })).toEqual(
      idSetOf([6]),
    );
  });

  runOrSkip('_starts_with with injection payload matches literally or empty', async () => {
    const ids = await idsOf({
      title: { _starts_with: "'; DROP TABLE extension;--" },
    });
    expect(ids).toEqual([]);
  });

  runOrSkip('empty _contains string matches all non-null titles', async () => {
    const all = await idsOf({});
    const empty = await idsOf({ title: { _contains: '' } });
    expect(empty.length).toBe(all.length);
  });

  runOrSkip('very long _eq string returns empty without crash', async () => {
    const long = 'x'.repeat(12000);
    await expect(idsOf({ title: { _eq: long } })).resolves.toEqual([]);
  });

  runOrSkip('O2M filter notes.body does not duplicate parent ids', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { notes: { body: { _eq: 'spam' } } },
      fields: ['_id'],
      sort: '_id',
    });
    const ids = (r.data as any[]).map((x) => String(x._id));
    expect(ids.length).toBe(new Set(ids).size);
    expect(ids.sort()).toEqual(idSetOf([0, 1]));
  });

  runOrSkip('M2M filter tags.label narrows to linked extensions (parity with SQL)', async () => {
    const ids = await idsOf({ tags: { label: { _eq: 'x' } } });
    expect(ids).toEqual(idSetOf([0, 2]));
  });

  runOrSkip('_contains NFC equals NFD tolerance or fail deterministically', async () => {
    const nfc = await idsOf({
      title: { _contains: 'Résumé'.normalize('NFC') },
    });
    const nfd = await idsOf({
      title: { _contains: 'Résumé'.normalize('NFD') },
    });
    expect(nfc).toEqual(idSetOf([5]));
    expect([[], idSetOf([5])]).toContainEqual(nfd);
  });

  runOrSkip('emoji in _contains never crashes and yields empty', async () => {
    await expect(idsOf({ title: { _contains: '🔥' } })).resolves.toEqual([]);
  });

  runOrSkip('self-reference category.parent resolves nested row', async () => {
    const r = await runPlan({
      tableName: 'category',
      filter: { _id: { _eq: catIds[2] } },
      fields: ['_id', 'name', 'parent._id', 'parent.name'],
    });
    const row = (r.data as any[])[0];
    expect(row.parent?.name).toBe('child');
    expect(String(row.parent?._id)).toBe(String(catIds[1]));
  });

  runOrSkip('owner relation with duplicate FK is fetched once, attached to both parents', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _in: [extIds[0], extIds[6]] } },
      fields: ['_id', 'owner._id', 'owner.name'],
      sort: '_id',
    });
    const rows = r.data as any[];
    expect(rows.length).toBe(2);
    expect(String(rows[0].owner?._id)).toBe(String(userIds[0]));
    expect(String(rows[1].owner?._id)).toBe(String(userIds[0]));
    expect(rows[0].owner.name).toBe('alice');
  });

  runOrSkip('all-null FK row yields null owner, no crash', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _eq: extIds[3] } },
      fields: ['_id', 'owner._id', 'owner.name'],
    });
    const row = (r.data as any[])[0];
    expect(row.owner).toBeNull();
  });

  runOrSkip('page/limit pagination works deterministically', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: {},
      fields: ['_id'],
      sort: 'title',
      limit: 2,
      page: 1,
    });
    expect((r.data as any[]).length).toBe(2);
  });

  runOrSkip('meta filterCount returns total count', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: {},
      fields: ['_id'],
      meta: 'filterCount',
    });
    expect(r.meta?.filterCount).toBe(7);
  });

  runOrSkip('fields with non-existent relation is ignored', async () => {
    await expect(
      runPlan({
        tableName: 'extension',
        filter: { _id: { _eq: extIds[0] } },
        fields: ['_id', 'notARelation.x'],
      }),
    ).resolves.toBeDefined();
  });

  runOrSkip('M2M zero-junction parent yields empty array', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _eq: extIds[3] } },
      fields: ['_id', 'tags._id'],
    });
    const row = (r.data as any[])[0];
    expect(row.tags).toEqual([]);
  });

  runOrSkip('top-level _and and _or together — both must hold', async () => {
    const ids = await idsOf({
      _and: [{ prio: { _gte: 0 } }, { prio: { _lte: 20 } }],
      _or: [{ title: { _eq: 'does-not-exist' } }],
    } as any);
    expect(ids).toEqual([]);
  });

  runOrSkip('unknown operator _null common mistake lists canonical operators', async () => {
    await expect(
      idsOf({ _and: [{ menu: { _null: true } }] } as any),
    ).rejects.toMatchObject({
      message: expect.stringContaining('Unsupported filter operator "_null"'),
    });
  });

  // Skipped: depends on M2M label filter end-to-end through executor, which is
  // a pre-existing parity gap (see "M2M filter tags.label narrows to linked
  // extensions (parity with SQL)" — broken prior to this suite expansion).
  test('_or across plain field and M2M relation branch', async () => {
    const ids = await idsOf({
      _or: [{ tags: { label: { _eq: 'y' } } }, { title: { _eq: 'delta' } }],
    });
    expect(ids).toEqual(idSetOf([0, 3]));
  });

  runOrSkip('limit 0 is treated as unbounded', async () => {
    const ids = await idsOf({}, { limit: 0 });
    expect(ids.length).toBe(7);
  });

  runOrSkip('negative limit normalizes (does not crash)', async () => {
    const ids = await idsOf({}, { limit: -3 });
    expect(ids.length).toBe(7);
  });

  runOrSkip('duplicate ids in _in list do not duplicate result rows', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _in: [extIds[0], extIds[0], extIds[1]] } },
      fields: ['_id'],
      sort: '_id',
    });
    const ids = (r.data as any[]).map((x) => String(x._id));
    expect(ids.sort()).toEqual(idSetOf([0, 1]));
  });

  runOrSkip('large _in array bounded to existing set', async () => {
    const big: any[] = [];
    for (let i = 0; i < 5000; i++) big.push(new ObjectId());
    big.push(...extIds);
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _in: big } },
      fields: ['_id'],
      sort: '_id',
    });
    const ids = (r.data as any[])
      .map((x) => String(x._id))
      .sort();
    expect(ids).toEqual(idSetOf([0, 1, 2, 3, 4, 5, 6]));
  });

  runOrSkip('fields [] returns rows with defaults', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _eq: extIds[0] } },
      fields: [],
    });
    expect((r.data as any[]).length).toBeGreaterThanOrEqual(1);
  });

  runOrSkip('fields ["*","title"] does not crash', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _eq: extIds[0] } },
      fields: ['*', 'title'],
    });
    expect((r.data as any[])[0].title).toBeDefined();
  });

  runOrSkip('nested object without operator key yields no matching rows', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { title: { nested: 'bad' } } as any,
      fields: ['_id'],
      sort: '_id',
    });
    expect((r.data as any[]).length).toBe(0);
  });

  runOrSkip('limit+sort parity with unbounded sort (first N)', async () => {
    const unlimited = await runPlan({
      tableName: 'extension',
      filter: { _id: { _in: extIds } },
      fields: ['_id', 'prio'],
      sort: '-prio',
    });
    const limited = await runPlan({
      tableName: 'extension',
      filter: { _id: { _in: extIds } },
      fields: ['_id', 'prio'],
      sort: '-prio',
      limit: 3,
    });
    const topIds = (unlimited.data as any[])
      .slice(0, 3)
      .map((r) => String(r._id));
    const limitedIds = (limited.data as any[]).map((r) => String(r._id));
    expect(limitedIds).toEqual(topIds);
  });

  runOrSkip('sort with bogus field name does not crash nor mutate data', async () => {
    await runPlan({
      tableName: 'extension',
      fields: ['_id'],
      sort: '"; db.extension.drop();--',
    }).catch(() => undefined);
    const remaining = await db.collection('extension').countDocuments({});
    expect(remaining).toBe(7);
  });

  runOrSkip('filter value containing injection payload is treated as literal', async () => {
    await runPlan({
      tableName: 'extension',
      filter: { title: { _eq: "'; db.extension.drop();--" } },
      fields: ['_id'],
    });
    const remaining = await db.collection('extension').countDocuments({});
    expect(remaining).toBe(7);
  });

  runOrSkip('O2M with duplicate children preserves all rows (no dedupe)', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _eq: extIds[0] } },
      fields: ['_id', 'notes._id', 'notes.body'],
    });
    const row = (r.data as any[])[0];
    expect(Array.isArray(row.notes)).toBe(true);
    expect(row.notes.length).toBe(3);
  });

  runOrSkip(
    'batch fetch across >5000 parent ids returns grouped children without loss',
    async () => {
      const r = await runPlan({
        tableName: 'chunk_parent',
        filter: {},
        fields: ['_id', 'children._id'],
        sort: '_id',
      });
      const rows = r.data as any[];
      expect(rows.length).toBe(6000);
      const withChild = rows.filter(
        (row) => Array.isArray(row.children) && row.children.length > 0,
      );
      expect(withChild.length).toBe(6000);
    },
    120000,
  );

  runOrSkip('depth-chain field beyond depth does not throw', async () => {
    await expect(
      runPlan({
        tableName: 'extension',
        filter: { _id: { _eq: extIds[0] } },
        fields: ['_id', 'menu.a.b.c.d.e.f'],
      }),
    ).resolves.toBeDefined();
  });

  // Skipped: depends on M2M field fetch through executor (same pre-existing
  // parity gap as the M2M label filter). batch-relation-fetcher unit tests
  // cover the standalone M2M fetch path.
  test('M2M with parent having multiple targets returns all targets', async () => {
    const r = await runPlan({
      tableName: 'extension',
      filter: { _id: { _eq: extIds[0] } },
      fields: ['_id', 'tags._id', 'tags.label'],
    });
    const row = (r.data as any[])[0];
    const labels = row.tags.map((t: any) => t.label).sort();
    expect(labels).toEqual(['x', 'y']);
  });
});
