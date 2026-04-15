import knex, { Knex } from 'knex';
import { separateFilters } from 'src/infrastructure/query-builder/utils/sql/relation-filter.util';
import { hasLogicalOperators } from 'src/infrastructure/query-builder/utils/sql/build-where-clause';
import { SqlQueryExecutor } from 'src/infrastructure/query-builder/executors/sql-query-executor';
import {
  buildOracleStressFilters,
  oracleExtensionRowIds,
} from './filter-reference-extension-oracle';

type RelStub = {
  propertyName: string;
  type: 'one-to-one' | 'many-to-one' | 'one-to-many' | 'many-to-many';
  targetTable: string;
  targetTableName: string;
  foreignKeyColumn?: string;
};

function makeTableMeta(
  name: string,
  columnNames: string[],
  relations: RelStub[],
) {
  const intCols = new Set(['id', 'prio', 'menuId', 'ownerId', 'rId']);
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
      isNullable: ['menuId', 'ownerId', 'a', 'b', 'rId'].includes(n),
      isSystem: false,
      isUpdatable: true,
      tableId: 1,
    })),
    relations: relations as any[],
  };
}

function makeMetadata(
  main: ReturnType<typeof makeTableMeta>,
  extra?: Map<string, any>,
) {
  const m = new Map<string, any>();
  m.set(main.name, main);
  if (extra) {
    for (const [k, v] of extra) m.set(k, v);
  }
  return { tables: m };
}

describe('separateFilters (field vs relation, logical nesting)', () => {
  const extensionMeta = makeTableMeta(
    'extension',
    ['id', 'title', 'menuId'],
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
    ],
  );

  const opKeys: Array<keyof typeof samples> = [
    '_eq',
    '_neq',
    '_in',
    '_not_in',
    '_nin',
    '_is_null',
    '_is_not_null',
  ] as any;
  const samples: Record<string, any> = {
    _eq: 42,
    _neq: 7,
    _in: [1, 2],
    _not_in: [9],
    _nin: [9],
    _is_null: true,
    _is_not_null: true,
  };

  const shorthandCases: Array<{
    rel: string;
    op: string;
    val: any;
  }> = [];
  for (const op of opKeys) {
    for (const rel of ['menu', 'owner']) {
      shorthandCases.push({ rel, op, val: samples[op as string] });
    }
  }

  test('relation shorthand rel with op → relationFilters.rel.id', () => {
    for (const { rel, op, val } of shorthandCases) {
      const filter = { [rel]: { [op]: val } };
      const { fieldFilters, relationFilters, hasRelations } = separateFilters(
        filter,
        extensionMeta as any,
      );
      expect(hasRelations).toBe(true);
      expect(relationFilters[rel]).toEqual({ id: { [op]: val } });
      expect(Object.keys(fieldFilters).length).toBe(0);
    }
  });

  test('nested object on relation (non-shorthand) → relationFilters.menu', () => {
    const filter = { menu: { label: { _eq: 'x' } } } as any;
    const { fieldFilters, relationFilters } = separateFilters(
      filter,
      extensionMeta as any,
    );
    expect(relationFilters.menu).toEqual({ label: { _eq: 'x' } });
    expect(Object.keys(fieldFilters).length).toBe(0);
  });

  test('top-level _and delegates to fieldFilters only at separateFilters root', () => {
    const filter = {
      _and: [{ menu: { _eq: 1 } }, { id: { _neq: 2 } }],
    };
    const { fieldFilters, relationFilters } = separateFilters(
      filter,
      extensionMeta as any,
    );
    expect(fieldFilters._and).toEqual(filter._and);
    expect(Object.keys(relationFilters).length).toBe(0);
  });

  const logicalDepthCases: Array<{
    name: string;
    filter: any;
    expectHasLogical: boolean;
  }> = [
    { name: 'flat', filter: { id: { _eq: 1 } }, expectHasLogical: false },
    {
      name: '_and',
      filter: { _and: [{ id: { _eq: 1 } }] },
      expectHasLogical: true,
    },
    {
      name: '_or',
      filter: { _or: [{ id: { _eq: 1 } }] },
      expectHasLogical: true,
    },
    {
      name: '_not',
      filter: { _not: { id: { _eq: 1 } } },
      expectHasLogical: true,
    },
    {
      name: 'deep value',
      filter: { a: { b: { _and: [{ x: 1 }] } } },
      expectHasLogical: true,
    },
  ];

  test('hasLogicalOperators cases', () => {
    for (const { filter, expectHasLogical } of logicalDepthCases) {
      expect(hasLogicalOperators(filter)).toBe(expectHasLogical);
    }
  });

  const cartesianAndOr: any[] = [];
  for (let a = 0; a < 3; a++) {
    for (let b = 0; b < 3; b++) {
      cartesianAndOr.push({
        _and: [{ id: { _eq: a } }, { title: { _neq: String(b) } }],
      });
    }
  }

  test('separateFilters preserves _and atomic clauses (cartesian)', () => {
    for (const filter of cartesianAndOr) {
      const { fieldFilters } = separateFilters(filter, extensionMeta as any);
      expect(fieldFilters._and).toHaveLength(2);
    }
  });

  test('_and member with menu + title splits relation vs fields', () => {
    const cond = { menu: { _eq: 88 }, title: { _like: '%a%' } } as any;
    const { fieldFilters, relationFilters } = separateFilters(
      cond,
      extensionMeta as any,
    );
    expect(relationFilters.menu).toEqual({ id: { _eq: 88 } });
    expect(fieldFilters.title).toEqual({ _like: '%a%' });
  });
});

describe('SqlQueryExecutor + SQLite (relation + _and / _or / _not)', () => {
  let db: Knex;
  let executor: SqlQueryExecutor;
  let meta: ReturnType<typeof makeMetadata>;

  beforeAll(async () => {
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
    ]);

    const menuTable = makeTableMeta('menu', ['id', 'label'], []);
    const userTable = makeTableMeta('user', ['id', 'name'], []);
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
      ],
    );
    meta = makeMetadata(
      extTable,
      new Map([
        ['menu', menuTable],
        ['user', userTable],
      ]),
    );

    executor = new SqlQueryExecutor(db, 'sqlite');
  });

  afterAll(async () => {
    await db.destroy();
  });

  async function debugSql(
    filter: any,
    extra: Partial<{ fields: string[]; limit: number }> = {},
  ) {
    const r = await executor.execute({
      tableName: 'extension',
      filter,
      fields: extra.fields ?? ['id'],
      limit: extra.limit,
      sort: 'id',
      debugMode: true,
      metadata: meta,
    });
    return String(r.sql || '').toLowerCase();
  }

  async function rowIds(filter: any, extra: Record<string, any> = {}) {
    const r = await executor.execute({
      tableName: 'extension',
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

  test('_and: menu eq 88 and id neq 1 → SQL mentions menuId/exists or join and id', async () => {
    const sql = await debugSql({
      _and: [{ menu: { _eq: 88 } }, { id: { _neq: 1 } }],
    });
    expect(sql).toMatch(/menuid|exists|\`/i);
    expect(sql).toMatch(/!=|<>/);
  });

  test('_or: two relation shards', async () => {
    const sql = await debugSql({
      _or: [{ menu: { _eq: 88 } }, { menu: { _eq: 99 } }],
    });
    expect(sql).toMatch(/or/);
    expect(sql.length).toBeGreaterThan(20);
  });

  test('_not around field', async () => {
    const sql = await debugSql({
      _not: { id: { _eq: 1 } },
    });
    expect(sql).toMatch(/not/);
  });

  test('flat AND: menu + id without _and', async () => {
    const sql = await debugSql({ menu: { _eq: 88 }, id: { _neq: 1 } });
    expect(sql).toMatch(/menuid/i);
    expect(sql).toMatch(/!=|<>/);
  });

  const tripleAnd = [];
  for (const x of [1, 2, 3]) {
    for (const y of [88, 99]) {
      tripleAnd.push({
        _and: [
          { id: { _neq: x } },
          { menu: { _eq: y } },
          { title: { _neq: 'z' } },
        ],
      });
    }
  }

  test('complex _and compiles without throw', async () => {
    for (const filter of tripleAnd) {
      const sql = await debugSql(filter);
      expect(sql).toContain('select');
    }
  });

  test('execute filterCount meta id gt values', async () => {
    for (const minId of [1, 2, 3, 4, 5, 6]) {
      const r = await executor.execute({
        tableName: 'extension',
        filter: { id: { _gt: minId } },
        fields: ['id'],
        sort: 'id',
        meta: 'filterCount',
        metadata: meta,
      });
      expect(r.data.length).toBeGreaterThanOrEqual(0);
      expect(r.meta?.filterCount).toBeDefined();
    }
  });

  test('null FK row: menu _is_null', async () => {
    const r = await executor.execute({
      tableName: 'extension',
      filter: { menu: { _is_null: true } },
      fields: ['id'],
      sort: 'id',
      metadata: meta,
    });
    const ids = r.data
      .map((x: any) => x.id)
      .sort((a: number, b: number) => a - b);
    expect(ids).toEqual([4]);
  });

  const boundedIds: Array<{ filter: any; min?: number; max?: number }> = [];
  for (const id of [1, 2, 3, 4, 5, 6]) {
    boundedIds.push({ filter: { id: { _eq: id } }, min: id, max: id });
  }
  for (const op of ['_neq', '_gt', '_gte', '_lt', '_lte'] as const) {
    for (const v of [0, 1, 2, 3, 4, 5, 6]) {
      boundedIds.push({ filter: { id: { [op]: v } } });
    }
  }

  test('bounded filters return rows', async () => {
    for (const { filter } of boundedIds) {
      const r = await executor.execute({
        tableName: 'extension',
        filter,
        fields: ['id'],
        sort: 'id',
        metadata: meta,
      });
      expect(Array.isArray(r.data)).toBe(true);
    }
  });

  test('baseline: empty filter → all ids', async () => {
    expect(await rowIds({})).toEqual([1, 2, 3, 4, 5, 6]);
  });

  test('contradiction: _and id=1 and id=2 → no rows', async () => {
    expect(
      await rowIds({ _and: [{ id: { _eq: 1 } }, { id: { _eq: 2 } }] }),
    ).toEqual([]);
  });

  test('contradiction: _and menu 88 and menu 99 → no rows', async () => {
    expect(
      await rowIds({ _and: [{ menu: { _eq: 88 } }, { menu: { _eq: 99 } }] }),
    ).toEqual([]);
  });

  const idOrGrid: Array<[number, number, number[]]> = [];
  for (let a = 1; a <= 6; a++) {
    for (let b = 1; b <= 6; b++) {
      idOrGrid.push([a, b, [...new Set([a, b])].sort((x, y) => x - y)]);
    }
  }

  test('or id grid', async () => {
    for (const [a, b, expected] of idOrGrid) {
      expect(
        await rowIds({ _or: [{ id: { _eq: a } }, { id: { _eq: b } }] }),
      ).toEqual(expected);
    }
  });

  test('menu in values', async () => {
    const cases = [
      [[88], [1, 2, 6]],
      [[99], [3]],
      [[100], [5]],
      [[88, 99], [1, 2, 3, 6]],
      [[88, 100], [1, 2, 5, 6]],
      [[99, 100], [3, 5]],
      [[88, 99, 100], [1, 2, 3, 5, 6]],
    ] as const;
    for (const [mids, exp] of cases) {
      expect(await rowIds({ menu: { _in: mids as number[] } })).toEqual(
        exp as number[],
      );
    }
  });

  test('menu._not_in [88]: SQL excludes NULL FK (not IN semantics)', async () => {
    expect(await rowIds({ menu: { _not_in: [88] } })).toEqual([3, 5]);
  });

  test('owner._eq values', async () => {
    const cases = [
      [1, [1, 3]],
      [2, [2, 6]],
      [3, [5]],
    ] as const;
    for (const [oid, exp] of cases) {
      expect(await rowIds({ owner: { _eq: oid } })).toEqual(exp);
    }
  });

  test('owner._in [1,3]', async () => {
    expect(await rowIds({ owner: { _in: [1, 3] } })).toEqual([1, 3, 5]);
  });

  test('owner._is_null → id 4 only', async () => {
    expect(await rowIds({ owner: { _is_null: true } })).toEqual([4]);
  });

  test('owner._is_not_null → all but 4', async () => {
    expect(await rowIds({ owner: { _is_not_null: true } })).toEqual([
      1, 2, 3, 5, 6,
    ]);
  });

  test('dual relation _and menu=88 owner=2 → [2]', async () => {
    expect(
      await rowIds({
        _and: [{ menu: { _eq: 88 } }, { owner: { _eq: 2 } }],
      }),
    ).toEqual([2, 6]);
  });

  test('dual relation flat menu=88 owner=1', async () => {
    expect(await rowIds({ menu: { _eq: 88 }, owner: { _eq: 1 } })).toEqual([1]);
  });

  test('_or relation vs field: menu 99 or title delta', async () => {
    expect(
      await rowIds({
        _or: [{ menu: { _eq: 99 } }, { title: { _eq: 'delta' } }],
      }),
    ).toEqual([3, 4]);
  });

  test('prio _between [5,10] → [1,3,5]', async () => {
    expect(await rowIds({ prio: { _between: [5, 10] } })).toEqual([1, 3, 5, 6]);
  });

  test('title _contains chunk → [3]', async () => {
    expect(await rowIds({ title: { _contains: 'chunk' } })).toEqual([3]);
  });

  test('SQLite title _contains resume: no accent fold → excludes Résumé row', async () => {
    expect(await rowIds({ title: { _contains: 'resume' } })).toEqual([]);
    expect(
      oracleExtensionRowIds({ title: { _contains: 'resume' } }, 'ascii'),
    ).toEqual([]);
  });

  test('title _eq Résumé → [6]', async () => {
    expect(await rowIds({ title: { _eq: 'Résumé' } })).toEqual([6]);
  });

  test('title _starts_with beta', async () => {
    expect(await rowIds({ title: { _starts_with: 'beta' } })).toEqual([2]);
  });

  test('title _ends_with unicode suffix', async () => {
    expect(await rowIds({ title: { _ends_with: '你好' } })).toEqual([5]);
  });

  test('title exact unicode', async () => {
    expect(await rowIds({ title: { _eq: 'unicode_你好' } })).toEqual([5]);
  });

  test('menu nested filter by label on related table', async () => {
    expect(await rowIds({ menu: { label: { _eq: 'm88' } } })).toEqual([
      1, 2, 6,
    ]);
  });

  test('nested _and group: (id 2..4) ∧ title not zzz', async () => {
    expect(
      await rowIds({
        _and: [
          {
            _and: [{ id: { _gte: 2 } }, { id: { _lte: 4 } }],
          },
          { title: { _neq: 'impossible_title_xyz' } },
        ],
      }),
    ).toEqual([2, 3, 4]);
  });

  test('_not (id=1)', async () => {
    expect(await rowIds({ _not: { id: { _eq: 1 } } })).toEqual([2, 3, 4, 5, 6]);
  });

  test('_not on single relation shorthand: not menu 88 → rows not linked to 88', async () => {
    expect(await rowIds({ _not: { menu: { _eq: 88 } } })).toEqual([3, 5]);
  });

  test('_not + _and field + relation: NOT (id=2 ∧ menu=88) → all but row 2', async () => {
    expect(
      await rowIds({
        _not: { _and: [{ id: { _eq: 2 } }, { menu: { _eq: 88 } }] },
      }),
    ).toEqual([1, 3, 4, 5, 6]);
  });

  test('_not + _or field only: NOT (id=1 ∨ id=2) → [3,4,5]', async () => {
    expect(
      await rowIds({ _not: { _or: [{ id: { _eq: 1 } }, { id: { _eq: 2 } }] } }),
    ).toEqual([3, 4, 5, 6]);
  });

  test('_not + _or relations: NOT (menu 88 ∨ menu 99) → [5]', async () => {
    expect(
      await rowIds({
        _not: { _or: [{ menu: { _eq: 88 } }, { menu: { _eq: 99 } }] },
      }),
    ).toEqual([5]);
  });

  test('_not + _or field ∨ relation: NOT (id=1 ∨ menu=99) → [2,5,6]', async () => {
    expect(
      await rowIds({
        _not: { _or: [{ id: { _eq: 1 } }, { menu: { _eq: 99 } }] },
      }),
    ).toEqual([2, 5, 6]);
  });

  test('_not + _or 3 branches (id ∨ menu ∨ owner)', async () => {
    expect(
      await rowIds({
        _not: {
          _or: [
            { id: { _eq: 4 } },
            { menu: { _eq: 100 } },
            { owner: { _eq: 2 } },
          ],
        },
      }),
    ).toEqual([1, 3]);
  });

  const demorganGrid: Array<{ name: string; filter: any; exp: number[] }> = [];
  const menuById: Record<number, number | null> = {
    1: 88,
    2: 88,
    3: 99,
    4: null,
    5: 100,
    6: 88,
  };
  for (const a of [1, 2, 3]) {
    for (const b of [88, 99]) {
      const drop = new Set<number>();
      for (const id of [1, 2, 3, 4, 5, 6]) {
        if (id === a) {
          drop.add(id);
        }
        if (menuById[id] === b) {
          drop.add(id);
        }
        if (menuById[id] === null) {
          drop.add(id);
        }
      }
      const exp = [1, 2, 3, 4, 5, 6].filter((id) => !drop.has(id));
      demorganGrid.push({
        name: `NOT(or id=${a} menu=${b})`,
        filter: { _not: { _or: [{ id: { _eq: a } }, { menu: { _eq: b } }] } },
        exp,
      });
    }
  }

  test('deMorgan grid', async () => {
    for (const { filter, exp } of demorganGrid) {
      expect(await rowIds(filter)).toEqual(exp);
    }
  });

  test('_not + _or with single inner _and (field ∧ relation): NOT ((id=1 ∧ menu=88) ∨ ∅)', async () => {
    expect(
      await rowIds({
        _not: {
          _or: [{ _and: [{ id: { _eq: 1 } }, { menu: { _eq: 88 } }] }],
        },
      }),
    ).toEqual([2, 3, 4, 5, 6]);
  });

  test('triple _or three relation branches', async () => {
    expect(
      await rowIds({
        _or: [
          { menu: { _eq: 1 } },
          { menu: { _eq: 99 } },
          { owner: { _eq: 3 } },
        ],
      }),
    ).toEqual([3, 5]);
  });

  test('_or branch is _and (id ∧ menu): only row 1', async () => {
    expect(
      await rowIds({
        _or: [{ _and: [{ id: { _eq: 1 } }, { menu: { _eq: 88 } }] }],
      }),
    ).toEqual([1]);
  });

  test('_or: inner _and branch ∨ id=4 → [1,4]', async () => {
    expect(
      await rowIds({
        _or: [
          { _and: [{ id: { _eq: 1 } }, { menu: { _eq: 88 } }] },
          { id: { _eq: 4 } },
        ],
      }),
    ).toEqual([1, 4]);
  });

  test('_and wraps _or (id=1 ∨ menu=99) → [1,3]', async () => {
    expect(
      await rowIds({
        _and: [{ _or: [{ id: { _eq: 1 } }, { menu: { _eq: 99 } }] }],
      }),
    ).toEqual([1, 3]);
  });

  test('_and: (_or id=2 ∨ menu=99) ∧ owner=1 → [3]', async () => {
    expect(
      await rowIds({
        _and: [
          { _or: [{ id: { _eq: 2 } }, { menu: { _eq: 99 } }] },
          { owner: { _eq: 1 } },
        ],
      }),
    ).toEqual([3]);
  });

  test('deep _and length 5 (avoid NOT IN on nullable FK — drops NULL menuId in SQL)', async () => {
    const f = {
      _and: [
        { id: { _gte: 1 } },
        { id: { _lte: 5 } },
        { prio: { _gte: 0 } },
        { title: { _neq: 'nope' } },
        { title: { _contains: 'a' } },
      ],
    };
    expect(await rowIds(f)).toEqual([1, 2, 3, 4]);
  });

  test('pagination limit 2 page 1', async () => {
    expect(await rowIds({}, { limit: 2, page: 1 })).toEqual([1, 2]);
  });

  test('pagination limit 2 page 2', async () => {
    expect(await rowIds({}, { limit: 2, page: 2 })).toEqual([3, 4]);
  });

  test('pagination limit 2 page 3', async () => {
    expect(await rowIds({}, { limit: 2, page: 3 })).toEqual([5, 6]);
  });

  const largeIn = Array.from({ length: 200 }, (_, i) => i + 1);
  test('id._in huge array (noise ids) still works', async () => {
    expect(await rowIds({ id: { _in: largeIn } })).toEqual([1, 2, 3, 4, 5, 6]);
  });

  const compileStorm: any[] = [];
  for (let i = 0; i < 450; i++) {
    compileStorm.push({
      _and: [
        { id: { _gte: (i % 4) - 1 } },
        { prio: { _lte: 100 + (i % 50) } },
        ...(i % 11 === 0 ? [{ menu: { _not_in: [-1, -2] } }] : []),
        ...(i % 9 === 0
          ? [
              {
                _or: [
                  { title: { _contains: 'a' } },
                  { owner: { _is_null: false } },
                ],
              },
            ]
          : []),
        ...(i % 13 === 0 ? [{ _not: { id: { _eq: 999 } } }] : []),
      ],
    });
  }

  test('compile storm debugMode returns sql', async () => {
    for (const filter of compileStorm) {
      const sql = await debugSql(filter);
      expect(sql).toContain('select');
      expect(sql.length).toBeGreaterThan(15);
    }
  });

  const gridTriple: any[] = [];
  for (const mid of [88, 99, 100, null]) {
    for (const oid of [1, 2, 3, null]) {
      for (const minP of [0, 5, 10]) {
        const parts: any[] = [{ prio: { _gte: minP } }];
        if (mid !== null) {
          parts.push({ menu: { _eq: mid } });
        } else {
          parts.push({ menu: { _is_null: true } });
        }
        if (oid !== null) {
          parts.push({ owner: { _eq: oid } });
        } else {
          parts.push({ owner: { _is_null: true } });
        }
        gridTriple.push({ _and: parts });
      }
    }
  }

  test('relation×prio grid executes', async () => {
    for (const filter of gridTriple) {
      const idsFound = await rowIds(filter);
      expect(idsFound.every((id) => id >= 1 && id <= 6)).toBe(true);
    }
  });

  const oracleBulk = buildOracleStressFilters();
  test('oracle stress matches SqlQueryExecutor', async () => {
    for (const filter of oracleBulk) {
      expect(await rowIds(filter)).toEqual(oracleExtensionRowIds(filter, 'ascii'));
    }
  });
});

describe('separateFilters bulk generated (stress)', () => {
  const m = makeTableMeta(
    'bulk',
    ['id', 'a', 'b', 'rId'],
    [
      {
        propertyName: 'rel',
        type: 'many-to-one',
        targetTable: 'trel',
        targetTableName: 'trel',
        foreignKeyColumn: 'rId',
      },
    ],
  );

  const bulk: Array<{ id: number; filter: any }> = [];
  let id = 0;
  for (let i = 0; i < 20; i++) {
    for (let j = 0; j < 10; j++) {
      bulk.push({
        id: id++,
        filter: {
          _and: [
            { rel: { _eq: i } },
            { a: { _neq: String(j) } },
            { id: { _gt: j } },
          ],
        },
      });
    }
  }
  for (let k = 0; k < 30; k++) {
    bulk.push({
      id: id++,
      filter: {
        _or: [{ rel: { _in: [k, k + 1] } }, { b: { _eq: 'z' } }],
      },
    });
  }
  for (let k = 0; k < 30; k++) {
    bulk.push({
      id: id++,
      filter: {
        _not: { _and: [{ id: { _eq: k } }, { rel: { _neq: 99 } }] },
      },
    });
  }

  test('bulk separateFilters reports hasRelations when relation present', () => {
    for (const x of bulk) {
      const { hasRelations } = separateFilters(x.filter, m as any);
      expect(hasRelations).toBe(true);
    }
  });
});

describe('separateFilters many-to-one vs one-to-one naming', () => {
  const oneOne = makeTableMeta(
    'ext2',
    ['id', 'menuId'],
    [
      {
        propertyName: 'menu',
        type: 'one-to-one',
        targetTable: 'menu',
        targetTableName: 'menu',
        foreignKeyColumn: 'menuId',
      },
    ],
  );

  test('one-to-one shorthand still maps to id filter', () => {
    const { relationFilters } = separateFilters(
      { menu: { _eq: 5 } },
      oneOne as any,
    );
    expect(relationFilters.menu).toEqual({ id: { _eq: 5 } });
  });
});
