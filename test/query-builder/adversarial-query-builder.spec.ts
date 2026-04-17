import knex, { Knex } from 'knex';
import { SqlQueryExecutor } from 'src/infrastructure/query-builder/executors/sql-query-executor';

type RelStub = {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  targetTableName: string;
  foreignKeyColumn?: string;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
};

function makeTableMeta(
  name: string,
  columnNames: string[],
  relations: RelStub[],
) {
  const intCols = new Set([
    'id',
    'prio',
    'menuId',
    'ownerId',
    'extensionId',
    'tagId',
    'order',
  ]);
  return {
    id: 1,
    name,
    isSystem: false,
    columns: columnNames.map((n, i) => ({
      id: i + 1,
      name: n,
      type: intCols.has(n) ? 'int' : 'varchar',
      isPrimary: n === 'id',
      isGenerated: n === 'id',
      isNullable: ['menuId', 'ownerId'].includes(n),
      isSystem: false,
      isUpdatable: true,
      tableId: 1,
    })),
    relations: relations as any[],
  };
}

function makeMetadata(
  main: ReturnType<typeof makeTableMeta>,
  extra: Map<string, any>,
) {
  const m = new Map<string, any>();
  m.set(main.name, main);
  for (const [k, v] of extra) m.set(k, v);
  return { tables: m };
}

describe('Adversarial Query Builder (SqlQueryExecutor + SQLite)', () => {
  let db: Knex;
  let executor: SqlQueryExecutor;
  let meta: ReturnType<typeof makeMetadata>;
  let warnSpy: jest.SpyInstance;

  beforeAll(async () => {
    warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
    db = knex({
      client: 'sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
    });

    await db.schema.createTable('menu', (t) => {
      t.increments('id').primary();
      t.string('label');
    });
    await db.schema.createTable('user', (t) => {
      t.increments('id').primary();
      t.string('name');
    });
    await db.schema.createTable('extension', (t) => {
      t.increments('id').primary();
      t.string('title');
      t.integer('prio').defaultTo(0);
      t.integer('menuId').nullable();
      t.integer('ownerId').nullable();
    });
    await db.schema.createTable('ext_note', (t) => {
      t.increments('id').primary();
      t.integer('extensionId').notNullable();
      t.string('body');
    });
    await db.schema.createTable('ext_tag', (t) => {
      t.increments('id').primary();
      t.string('label');
    });
    await db.schema.createTable('ext_tag_junction', (t) => {
      t.integer('extensionId').notNullable();
      t.integer('tagId').notNullable();
      t.primary(['extensionId', 'tagId']);
    });
    await db.schema.createTable('kw_table', (t) => {
      t.increments('id').primary();
      t.integer('order');
    });
    await db.schema.createTable('category', (t) => {
      t.increments('id').primary();
      t.string('name');
      t.integer('parentId').nullable();
    });
    await db.schema.createTable('ext_profile', (t) => {
      t.increments('id').primary();
      t.integer('extensionId').notNullable();
      t.string('bio');
    });
    await db.schema.createTable('chunk_parent', (t) => {
      t.increments('id').primary();
    });
    await db.schema.createTable('chunk_child', (t) => {
      t.increments('id').primary();
      t.integer('parentId').notNullable();
    });

    await db('menu').insert([
      { id: 1, label: 'm1' },
      { id: 88, label: 'm88' },
      { id: 99, label: 'm99' },
      { id: 100, label: "o'reilly" },
    ]);
    await db('user').insert([
      { id: 1, name: 'alice' },
      { id: 2, name: 'bob' },
      { id: 3, name: 'carol' },
    ]);
    await db('extension').insert([
      { id: 1, title: 'alpha', prio: 10, menuId: 88, ownerId: 1 },
      { id: 2, title: 'beta', prio: 20, menuId: 88, ownerId: 2 },
      { id: 3, title: 'gamma_chunk', prio: 5, menuId: 99, ownerId: 1 },
      { id: 4, title: 'delta', prio: 0, menuId: null, ownerId: null },
      { id: 5, title: 'unicode_你好', prio: 7, menuId: 100, ownerId: 3 },
      { id: 6, title: 'Résumé', prio: 8, menuId: 88, ownerId: 2 },
      {
        id: 7,
        title: '100%_done_literal',
        prio: 1,
        menuId: 88,
        ownerId: 1,
      },
    ]);
    await db('ext_note').insert([
      { id: 1, extensionId: 1, body: 'spam' },
      { id: 2, extensionId: 1, body: 'ok' },
      { id: 3, extensionId: 2, body: 'spam' },
    ]);
    await db('ext_tag').insert([
      { id: 1, label: 'x' },
      { id: 2, label: 'y' },
    ]);
    await db('ext_tag_junction').insert([
      { extensionId: 1, tagId: 1 },
      { extensionId: 1, tagId: 2 },
      { extensionId: 3, tagId: 1 },
    ]);
    await db('kw_table').insert([{ id: 1, order: 42 }]);
    await db('category').insert([
      { id: 1, name: 'root', parentId: null },
      { id: 2, name: 'child', parentId: 1 },
      { id: 3, name: 'grandchild', parentId: 2 },
    ]);
    await db('ext_profile').insert([
      { id: 1, extensionId: 1, bio: 'first' },
      { id: 2, extensionId: 1, bio: 'dup-corruption' },
    ]);
    const bigParents = Array.from({ length: 6000 }, (_, i) => ({ id: i + 1 }));
    await db.batchInsert('chunk_parent', bigParents, 500);
    const bigChildren = bigParents.map((p) => ({
      parentId: p.id,
    }));
    await db.batchInsert('chunk_child', bigChildren, 500);

    const menuTable = makeTableMeta('menu', ['id', 'label'], []);
    const userTable = makeTableMeta('user', ['id', 'name'], []);
    const extNoteTable = makeTableMeta(
      'ext_note',
      ['id', 'extensionId', 'body'],
      [],
    );
    const extTagTable = makeTableMeta('ext_tag', ['id', 'label'], []);
    const extJunctionTable = makeTableMeta(
      'ext_tag_junction',
      ['extensionId', 'tagId'],
      [],
    );
    const kwTable = makeTableMeta('kw_table', ['id', 'order'], []);
    const categoryTable = makeTableMeta(
      'category',
      ['id', 'name', 'parentId'],
      [
        {
          propertyName: 'parent',
          type: 'many-to-one',
          targetTable: 'category',
          targetTableName: 'category',
          foreignKeyColumn: 'parentId',
        },
      ],
    );
    const extProfileTable = makeTableMeta(
      'ext_profile',
      ['id', 'extensionId', 'bio'],
      [],
    );
    const chunkParentTable = makeTableMeta(
      'chunk_parent',
      ['id'],
      [
        {
          propertyName: 'children',
          type: 'one-to-many',
          targetTable: 'chunk_child',
          targetTableName: 'chunk_child',
          foreignKeyColumn: 'parentId',
        },
      ],
    );
    const chunkChildTable = makeTableMeta(
      'chunk_child',
      ['id', 'parentId'],
      [],
    );

    const extTable = makeTableMeta(
      'extension',
      ['id', 'title', 'prio', 'menuId', 'ownerId'],
      [
        {
          propertyName: 'menu',
          type: 'many-to-one',
          targetTable: 'menu',
          targetTableName: 'menu',
          foreignKeyColumn: 'menuId',
        },
        {
          propertyName: 'owner',
          type: 'many-to-one',
          targetTable: 'user',
          targetTableName: 'user',
          foreignKeyColumn: 'ownerId',
        },
        {
          propertyName: 'notes',
          type: 'one-to-many',
          targetTable: 'ext_note',
          targetTableName: 'ext_note',
          foreignKeyColumn: 'extensionId',
        },
        {
          propertyName: 'tags',
          type: 'many-to-many',
          targetTable: 'ext_tag',
          targetTableName: 'ext_tag',
          junctionTableName: 'ext_tag_junction',
          junctionSourceColumn: 'extensionId',
          junctionTargetColumn: 'tagId',
        },
      ],
    );

    meta = makeMetadata(
      extTable,
      new Map<string, any>([
        ['menu', menuTable],
        ['user', userTable],
        ['ext_note', extNoteTable],
        ['ext_tag', extTagTable],
        ['ext_tag_junction', extJunctionTable],
        ['kw_table', kwTable],
        ['category', categoryTable],
        ['ext_profile', extProfileTable],
        ['chunk_parent', chunkParentTable],
        ['chunk_child', chunkChildTable],
      ]),
    );

    executor = new SqlQueryExecutor(db, 'sqlite', undefined, 6);
  });

  afterAll(async () => {
    warnSpy.mockRestore();
    await db.destroy();
  });

  async function rowIds(
    filter: any,
    extra: Record<string, any> = {},
    tableName = 'extension',
  ) {
    const r = await executor.execute({
      tableName,
      filter,
      fields: ['id'],
      sort: 'id',
      metadata: meta,
      ...extra,
    });
    return (r.data as any[])
      .map((x: any) => x.id)
      .sort((a: number, b: number) => a - b);
  }

  test('deep nested logical filter (_and / _or / triple _not) returns expected ids', async () => {
    const filter = {
      _and: [
        { _or: [{ id: { _eq: 1 } }, { id: { _eq: 4 } }] },
        {
          _not: {
            _not: {
              _not: { id: { _eq: 2 } },
            },
          },
        },
      ],
    };
    expect(await rowIds(filter)).toEqual([1, 4]);
  });

  test('_and: [] behaves as unconstrained logical filter (matches all rows)', async () => {
    expect(await rowIds({ _and: [] })).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  test('_or with single clause matches same as flat clause', async () => {
    const a = await rowIds({ _or: [{ title: { _eq: 'alpha' } }] });
    const b = await rowIds({ title: { _eq: 'alpha' } });
    expect(a).toEqual(b);
  });

  test('_not: {} does not constrain', async () => {
    expect(await rowIds({ _not: {} })).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  test('menuId null / _eq null / _is_null on column FK — record actual behavior', async () => {
    const shorthand = await rowIds({ menuId: null });
    const eqNull = await rowIds({ menuId: { _eq: null } });
    const isNull = await rowIds({ menuId: { _is_null: true } });
    expect(isNull).toEqual([4]);
    expect([[], [4]]).toContainEqual(shorthand);
    expect([[], [4]]).toContainEqual(eqNull);
  });

  test('M2O menu _is_null matches FK null; menu _eq null is not equivalent here', async () => {
    const eqNull = await rowIds({ menu: { _eq: null } });
    const isNull = await rowIds({ menu: { _is_null: true } });
    expect(eqNull).toEqual([]);
    expect(isNull).toEqual([4]);
  });

  test('id _in empty array returns no rows', async () => {
    expect(await rowIds({ id: { _in: [] } })).toEqual([]);
  });

  test('id _in includes null — SQL semantics (null never matches IN)', async () => {
    expect(await rowIds({ id: { _in: [null, 1, 2] } })).toEqual([1, 2]);
  });

  test('mutually exclusive _and on id returns empty', async () => {
    expect(
      await rowIds({ _and: [{ id: { _eq: 1 } }, { id: { _eq: 2 } }] }),
    ).toEqual([]);
  });

  test('top-level _and and _or together — both must hold as implicit AND of roots', async () => {
    const ids = await rowIds({
      _and: [{ id: { _gte: 1 } }, { id: { _lte: 6 } }],
      _or: [{ id: { _eq: 99 } }],
    } as any);
    expect(ids).toEqual([]);
  });

  test('unknown operator _weird throws BadRequest listing supported operators', async () => {
    await expect(rowIds({ title: { _weird: 1 } })).rejects.toMatchObject({
      message: expect.stringContaining('Unsupported filter operator "_weird"'),
    });
    await expect(rowIds({ title: { _weird: 1 } })).rejects.toMatchObject({
      message: expect.stringContaining('_is_null'),
    });
  });

  test('unknown operator _null (common mistake) throws with canonical list', async () => {
    await expect(
      rowIds({ _and: [{ menu: { _null: true } }] } as any),
    ).rejects.toMatchObject({
      message: expect.stringContaining('Unsupported filter operator "_null"'),
    });
  });

  test('_eq preserves literal percent and underscore in title', async () => {
    expect(await rowIds({ title: { _eq: '100%_done_literal' } })).toEqual([7]);
  });

  test('_contains matches stable substring on row 7', async () => {
    expect(await rowIds({ title: { _contains: 'done_literal' } })).toEqual([7]);
  });

  test('_starts_with with quote-like payload matches literally or returns empty', async () => {
    const ids = await rowIds({
      title: { _starts_with: "'; DROP TABLE extension;--" },
    });
    expect(ids).toEqual([]);
  });

  test('_contains matches accented title (SQLite lower LIKE)', async () => {
    expect(await rowIds({ title: { _contains: 'résumé' } })).toEqual([6]);
  });

  test('_contains empty string matches all titles (non-null)', async () => {
    const all = await rowIds({});
    const withEmpty = await rowIds({ title: { _contains: '' } });
    expect(withEmpty.length).toBe(all.length);
  });

  test('very long _eq string returns empty without throwing', async () => {
    const long = 'x'.repeat(12000);
    await expect(rowIds({ title: { _eq: long } })).resolves.toEqual([]);
  });

  test('O2M filter notes.body does not duplicate parent ids', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { notes: { body: { _eq: 'spam' } } },
      fields: ['id'],
      sort: 'id',
      metadata: meta,
    });
    const ids = (r.data as any[]).map((x) => x.id);
    expect(ids).toEqual([1, 2]);
    expect(new Set(ids).size).toBe(ids.length);
  });

  test('M2M filter tags.label narrows to extensions linked to tag x', async () => {
    expect(await rowIds({ tags: { label: { _eq: 'x' } } })).toEqual([1, 3]);
  });

  test('_or across field and M2M branch', async () => {
    expect(
      await rowIds({
        _or: [{ tags: { label: { _eq: 'y' } } }, { id: { _eq: 4 } }],
      }),
    ).toEqual([1, 4]);
  });

  test('reserved column name order on kw_table — filter and sort', async () => {
    const r = await executor.execute({
      tableName: 'kw_table',
      filter: { order: { _eq: 42 } },
      fields: ['id', 'order'],
      sort: '-order',
      metadata: meta,
    });
    expect(r.data).toHaveLength(1);
    expect((r.data as any[])[0].order).toBe(42);
  });

  test('limit 0 is skipped (executor only applies limit when > 0)', async () => {
    expect(await rowIds({}, { limit: 0 })).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  test('negative limit normalizes to 0 then skipped like limit 0', async () => {
    expect(await rowIds({}, { limit: -3 })).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  test('page 1 limit 2 slices deterministically', async () => {
    expect(await rowIds({}, { limit: 2, page: 1 })).toEqual([1, 2]);
  });

  test('meta filterCount with empty filter returns total', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: {},
      fields: ['id'],
      sort: 'id',
      meta: 'filterCount',
      metadata: meta,
    });
    expect(r.meta?.filterCount).toBe(7);
  });

  test('duplicate ids in _in list do not duplicate result rows', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _in: [1, 1, 2] } },
      fields: ['id'],
      sort: 'id',
      metadata: meta,
    });
    expect((r.data as any[]).map((x) => x.id)).toEqual([1, 2]);
  });

  test('large _in array still returns bounded extension set', async () => {
    const big = Array.from({ length: 8000 }, (_, i) => i + 1);
    expect(await rowIds({ id: { _in: big } })).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  test('fields array empty still returns rows (defaults)', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _eq: 1 } },
      fields: [],
      metadata: meta,
    });
    expect((r.data as any[]).length).toBeGreaterThanOrEqual(1);
  });

  test('fields ["*","title"] returns data without throwing', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _eq: 1 } },
      fields: ['*', 'title'],
      metadata: meta,
    });
    expect((r.data as any[])[0].title).toBeDefined();
  });

  test('numeric string FK menuId matches same as number when column is int', async () => {
    const a = await rowIds({ menuId: { _eq: 88 } });
    const b = await rowIds({ menuId: { _eq: '88' as any } });
    expect(a).toEqual(b);
  });

  test('nested object without operator key yields no scalar conditions', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { title: { nested: 'bad' } } as any,
      fields: ['id'],
      sort: 'id',
      metadata: meta,
    });
    expect((r.data as any[]).length).toBe(0);
  });

  test('CTE path (limit + sort) returns same rows as non-CTE', async () => {
    const noCte = await executor.execute({
      tableName: 'extension',
      filter: { id: { _gte: 1 } },
      fields: ['id', 'title'],
      sort: '-prio',
      metadata: meta,
    });
    const cte = await executor.execute({
      tableName: 'extension',
      filter: { id: { _gte: 1 } },
      fields: ['id', 'title'],
      sort: '-prio',
      limit: 5,
      metadata: meta,
    });
    const noCteIds = (noCte.data as any[]).slice(0, 5).map((r) => r.id);
    const cteIds = (cte.data as any[]).map((r) => r.id);
    expect(cteIds).toEqual(noCteIds);
  });

  test('sort with bogus field name does not inject SQL (errors or resolves, table intact)', async () => {
    await executor
      .execute({
        tableName: 'extension',
        fields: ['id'],
        sort: '"; DROP TABLE extension;--',
        metadata: meta,
      })
      .catch(() => undefined);
    const check = await db('extension').count<{ c: number }[]>({ c: '*' });
    expect(Number((check as any)[0].c)).toBe(7);
  });

  test('filter value containing quote payload is parameterised, not injected', async () => {
    await executor.execute({
      tableName: 'extension',
      filter: { title: { _eq: "'; DROP TABLE extension;--" } },
      fields: ['id'],
      metadata: meta,
    });
    const check = await db('extension').count<{ c: number }[]>({ c: '*' });
    expect(Number((check as any)[0].c)).toBe(7);
  });

  test('self-reference category.parent resolves nested row via batch fetch', async () => {
    const r = await executor.execute({
      tableName: 'category',
      filter: { id: { _eq: 3 } },
      fields: ['id', 'name', 'parent.id', 'parent.name'],
      metadata: meta,
    });
    const row = (r.data as any[])[0];
    expect(row.id).toBe(3);
    expect(row.parent?.id).toBe(2);
    expect(row.parent?.name).toBe('child');
  });

  test('O2O-ish inverse with duplicate children is not silently deduplicated in O2M', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _eq: 1 } },
      fields: ['id', 'notes.id'],
      metadata: meta,
    });
    const row = (r.data as any[])[0];
    expect(Array.isArray(row.notes)).toBe(true);
    expect(row.notes.length).toBe(2);
  });

  test('batch fetch across >5000 parent ids returns grouped children without loss', async () => {
    const r = await executor.execute({
      tableName: 'chunk_parent',
      filter: {},
      fields: ['id', 'children.id'],
      sort: 'id',
      metadata: meta,
    });
    const rows = r.data as any[];
    expect(rows.length).toBe(6000);
    const withChild = rows.filter(
      (row) => Array.isArray(row.children) && row.children.length > 0,
    );
    expect(withChild.length).toBe(6000);
    expect(rows[0].children[0]).toBeDefined();
  });

  test('fields referencing non-existent relation is ignored, not thrown', async () => {
    await expect(
      executor.execute({
        tableName: 'extension',
        filter: { id: { _eq: 1 } },
        fields: ['id', 'notARelation.x'],
        metadata: meta,
      }),
    ).resolves.toBeDefined();
  });

  test('depth-chain field beyond maxQueryDepth does not throw and returns base row', async () => {
    await expect(
      executor.execute({
        tableName: 'extension',
        filter: { id: { _eq: 1 } },
        fields: ['id', 'menu.a.b.c.d.e.f'],
        metadata: meta,
      }),
    ).resolves.toBeDefined();
  });

  test('unicode NFC vs NFD _contains on accented title', async () => {
    const nfc = await rowIds({
      title: { _contains: 'Résumé'.normalize('NFC') },
    });
    const nfd = await rowIds({
      title: { _contains: 'Résumé'.normalize('NFD') },
    });
    expect(nfc).toEqual([6]);
    expect([[], [6]]).toContainEqual(nfd);
  });

  test('emoji in _contains does not crash and matches nothing', async () => {
    await expect(rowIds({ title: { _contains: '🔥' } })).resolves.toEqual([]);
  });

  test('duplicate owner FK values are deduplicated before fetchOwner', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _in: [1, 7] } },
      fields: ['id', 'owner.id', 'owner.name'],
      sort: 'id',
      metadata: meta,
    });
    const rows = r.data as any[];
    expect(rows.length).toBe(2);
    expect(rows[0].owner?.id).toBe(1);
    expect(rows[1].owner?.id).toBe(1);
    expect(rows[0].owner).toEqual(rows[1].owner);
  });

  test('all-null FK owner relation yields null on every row, no crash', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _eq: 4 } },
      fields: ['id', 'owner.id', 'owner.name'],
      metadata: meta,
    });
    const row = (r.data as any[])[0];
    expect(row.id).toBe(4);
    expect(row.owner).toBeNull();
  });

  test('M2M with parent having multiple targets returns all targets (no junction dedupe bug)', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _eq: 1 } },
      fields: ['id', 'tags.id', 'tags.label'],
      metadata: meta,
    });
    const row = (r.data as any[])[0];
    expect(row.tags.map((t: any) => t.id).sort()).toEqual([1, 2]);
  });

  test('M2M with parent having zero junction rows yields empty array', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { id: { _eq: 4 } },
      fields: ['id', 'tags.id'],
      metadata: meta,
    });
    const row = (r.data as any[])[0];
    expect(row.tags).toEqual([]);
  });
});
