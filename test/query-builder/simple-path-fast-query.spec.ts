import knex, { Knex } from 'knex';
import { SqlQueryExecutor, QueryPlanner } from '@enfyra/kernel';

let db: Knex;

const metadata = {
  tables: new Map([
    [
      'products',
      {
        name: 'products',
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'name', type: 'varchar' },
          { name: 'price', type: 'integer' },
          { name: 'active', type: 'boolean' },
          { name: 'category', type: 'varchar' },
          { name: 'rating', type: 'float' },
          { name: 'tags', type: 'json' },
          { name: 'createdAt', type: 'datetime' },
        ],
        relations: [],
      },
    ],
    [
      'orders',
      {
        name: 'orders',
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'total', type: 'integer' },
          { name: 'status', type: 'varchar' },
          { name: 'productId', type: 'integer' },
          { name: 'quantity', type: 'integer' },
          { name: 'note', type: 'text' },
        ],
        relations: [
          {
            propertyName: 'product',
            type: 'many-to-one',
            targetTableName: 'products',
            foreignKeyColumn: 'productId',
          },
        ],
      },
    ],
    [
      'empty_table',
      {
        name: 'empty_table',
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'value', type: 'varchar' },
        ],
        relations: [],
      },
    ],
  ]),
  tablesList: [],
};

function plan(tableName: string, opts: any = {}) {
  const planner = new QueryPlanner();
  return planner.plan({
    tableName,
    fields: opts.fields,
    filter: opts.filter,
    sort: opts.sort,
    page: opts.page,
    limit: opts.limit,
    meta: opts.meta,
    metadata,
    dbType: 'sqlite' as any,
  });
}

function executor() {
  return new SqlQueryExecutor(db, 'sqlite');
}

beforeAll(async () => {
  db = knex({
    client: 'sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
  });

  await db.schema.createTable('products', (t) => {
    t.increments('id').primary();
    t.string('name');
    t.integer('price');
    t.boolean('active').defaultTo(true);
    t.string('category');
    t.float('rating');
    t.json('tags');
    t.text('createdAt');
  });

  await db.schema.createTable('orders', (t) => {
    t.increments('id').primary();
    t.integer('total');
    t.string('status');
    t.integer('productId').references('id').inTable('products');
    t.integer('quantity');
    t.text('note');
  });

  await db.schema.createTable('empty_table', (t) => {
    t.increments('id').primary();
    t.string('value');
  });

  const products = [];
  for (let i = 1; i <= 100; i++) {
    products.push({
      id: i,
      name: `Product ${i}`,
      price: i * 10,
      active: i % 3 !== 0,
      category: ['electronics', 'books', 'clothing', 'food', 'toys'][
        (i - 1) % 5
      ],
      rating: (i % 5) + 1,
      tags: JSON.stringify([`tag${i}`, `tag${i + 1}`]),
      createdAt: new Date(2024, 0, i).toISOString(),
    });
  }
  await db('products').insert(products);

  const orders = [];
  for (let i = 1; i <= 50; i++) {
    orders.push({
      id: i,
      total: i * 100,
      status: ['pending', 'shipped', 'delivered', 'cancelled'][(i - 1) % 4],
      productId: i,
      quantity: i,
      note: i % 2 === 0 ? `Note for order ${i}` : null,
    });
  }
  await db('orders').insert(orders);
});

afterAll(async () => {
  await db.destroy();
});

// ═══════════════════════════════════════════════════════════════════
// Fast path activation conditions
// ═══════════════════════════════════════════════════════════════════

describe('executeSimple — activation guard', () => {
  it('activates for explicit fields, no relations', async () => {
    const p = plan('products', { fields: 'id,name', limit: 3 });
    const exec = executor();
    const result = await exec.execute({
      tableName: 'products',
      fields: 'id,name',
      limit: 3,
      metadata,
      plan: p,
    });
    expect(result.data).toHaveLength(3);
    expect(result.data[0]).toHaveProperty('id');
    expect(result.data[0]).toHaveProperty('name');
    expect(result.data[0]).not.toHaveProperty('price');
  });

  it('falls back to full path for fields=*', async () => {
    const p = plan('products', { fields: '*' });
    const exec = executor();
    const result = await exec.execute({
      tableName: 'products',
      fields: '*',
      limit: 1,
      metadata,
      plan: p,
    });
    expect(result.data).toHaveLength(1);
    expect(result.data[0]).toHaveProperty('price');
    expect(result.data[0]).toHaveProperty('category');
  });

  it('falls back for no fields specified', async () => {
    const p = plan('products', {});
    const exec = executor();
    const result = await exec.execute({
      tableName: 'products',
      limit: 1,
      metadata,
      plan: p,
    });
    expect(result.data).toHaveLength(1);
  });

  it('falls back when table has relations', async () => {
    const p = plan('orders', { fields: 'id,total' });
    const exec = executor();
    const result = await exec.execute({
      tableName: 'orders',
      fields: 'id,total',
      limit: 2,
      metadata,
      plan: p,
    });
    expect(result.data).toHaveLength(2);
    expect(result.data[0]).toHaveProperty('id');
    expect(result.data[0]).toHaveProperty('total');
  });

  it('falls back when deep is provided', async () => {
    const p = plan('products', { fields: 'id,name' });
    const exec = executor();
    const result = await exec.execute({
      tableName: 'products',
      fields: 'id,name',
      limit: 1,
      metadata,
      plan: p,
      deep: { product: { fields: ['id'] } },
    });
    expect(result.data).toHaveLength(1);
  });
});

// ═══════════════════════════════════════════════════════════════════
// _eq
// ═══════════════════════════════════════════════════════════════════

describe('simple path — _eq', () => {
  it('exact match on string', async () => {
    const p = plan('products', {
      fields: 'id,name',
      filter: { name: { _eq: 'Product 1' } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { name: { _eq: 'Product 1' } },
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(1);
    expect(r.data[0].name).toBe('Product 1');
  });

  it('exact match on integer', async () => {
    const p = plan('products', {
      fields: 'id,name',
      filter: { id: { _eq: 5 } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { id: { _eq: 5 } },
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(1);
    expect(r.data[0].id).toBe(5);
  });

  it('no match returns empty', async () => {
    const p = plan('products', {
      fields: 'id,name',
      filter: { name: { _eq: 'Nonexistent' } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { name: { _eq: 'Nonexistent' } },
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(0);
  });
});

// ═══════════════════════════════════════════════════════════════════
// _neq
// ═══════════════════════════════════════════════════════════════════

describe('simple path — _neq', () => {
  it('excludes matching records', async () => {
    const p = plan('products', {
      fields: 'id',
      filter: { category: { _neq: 'electronics' } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      filter: { category: { _neq: 'electronics' } },
      metadata,
      plan: p,
    });
    expect(r.data.every((_row: any) => r.data.length > 0)).toBe(true);
    expect(r.data.length).toBeLessThan(100);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Comparison operators
// ═══════════════════════════════════════════════════════════════════

describe('simple path — _gt _gte _lt _lte', () => {
  it('_gt filters greater than', async () => {
    const p = plan('products', {
      fields: 'id,price',
      filter: { price: { _gt: 900 } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      filter: { price: { _gt: 900 } },
      metadata,
      plan: p,
    });
    expect(r.data.length).toBeGreaterThan(0);
    expect(r.data.every((row: any) => row.price > 900)).toBe(true);
  });

  it('_gte includes boundary', async () => {
    const p = plan('products', {
      fields: 'id,price',
      filter: { price: { _gte: 900 } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      filter: { price: { _gte: 900 } },
      metadata,
      plan: p,
    });
    expect(r.data.some((row: any) => row.price === 900)).toBe(true);
  });

  it('_lt filters less than', async () => {
    const p = plan('products', {
      fields: 'id,price',
      filter: { price: { _lt: 50 } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      filter: { price: { _lt: 50 } },
      metadata,
      plan: p,
    });
    expect(r.data.length).toBe(4);
    expect(r.data.every((row: any) => row.price < 50)).toBe(true);
  });

  it('_lte includes boundary', async () => {
    const p = plan('products', {
      fields: 'id,price',
      filter: { price: { _lte: 50 } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      filter: { price: { _lte: 50 } },
      metadata,
      plan: p,
    });
    expect(r.data.some((row: any) => row.price === 50)).toBe(true);
  });
});

// ═══════════════════════════════════════════════════════════════════
// _in / _not_in
// ═══════════════════════════════════════════════════════════════════

describe('simple path — _in _not_in', () => {
  it('_in matches multiple values', async () => {
    const p = plan('products', {
      fields: 'id,category',
      filter: { category: { _in: ['electronics', 'books'] } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,category',
      filter: { category: { _in: ['electronics', 'books'] } },
      metadata,
      plan: p,
    });
    expect(r.data.length).toBe(40);
    expect(
      r.data.every((row: any) =>
        ['electronics', 'books'].includes(row.category),
      ),
    ).toBe(true);
  });

  it('_in with empty array returns nothing', async () => {
    const p = plan('products', {
      fields: 'id',
      filter: { category: { _in: [] } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      filter: { category: { _in: [] } },
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(0);
  });

  it('_not_in excludes values', async () => {
    const p = plan('products', {
      fields: 'id,category',
      filter: { category: { _not_in: ['electronics'] } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,category',
      filter: { category: { _not_in: ['electronics'] } },
      metadata,
      plan: p,
    });
    expect(r.data.every((row: any) => row.category !== 'electronics')).toBe(
      true,
    );
  });
});

// ═══════════════════════════════════════════════════════════════════
// String operators
// ═══════════════════════════════════════════════════════════════════

describe('simple path — _contains _starts_with _ends_with', () => {
  it('_contains matches substring', async () => {
    const p = plan('products', {
      fields: 'id,name',
      filter: { name: { _contains: 'Product 1' } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { name: { _contains: 'Product 1' } },
      metadata,
      plan: p,
    });
    expect(r.data.length).toBeGreaterThan(0);
    expect(r.data.every((row: any) => row.name.includes('Product 1'))).toBe(
      true,
    );
  });

  it('_starts_with matches prefix', async () => {
    const p = plan('products', {
      fields: 'id,name',
      filter: { category: { _starts_with: 'elec' } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { category: { _starts_with: 'elec' } },
      metadata,
      plan: p,
    });
    expect(r.data.length).toBeGreaterThan(0);
  });

  it('_ends_with matches suffix', async () => {
    const p = plan('products', {
      fields: 'id,name',
      filter: { name: { _ends_with: '99' } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { name: { _ends_with: '99' } },
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(1);
    expect(r.data[0].name).toBe('Product 99');
  });
});

// ═══════════════════════════════════════════════════════════════════
// _is_null / _is_not_null
// ═══════════════════════════════════════════════════════════════════

describe('simple path — _is_null _is_not_null', () => {
  it('_is_null finds nulls', async () => {
    const p = plan('orders', {
      fields: 'id,note',
      filter: { note: { _is_null: true } },
    });
    const r = await executor().execute({
      tableName: 'orders',
      fields: 'id,note',
      filter: { note: { _is_null: true } },
      metadata,
      plan: p,
    });
    expect(
      r.data.every((row: any) => row.note === null || row.note === undefined),
    ).toBe(true);
  });

  it('_is_not_null finds non-nulls', async () => {
    const p = plan('orders', {
      fields: 'id,note',
      filter: { note: { _is_not_null: true } },
    });
    const r = await executor().execute({
      tableName: 'orders',
      fields: 'id,note',
      filter: { note: { _is_not_null: true } },
      metadata,
      plan: p,
    });
    expect(
      r.data.every((row: any) => row.note !== null && row.note !== undefined),
    ).toBe(true);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Logical operators (_and, _or, _not)
// ═══════════════════════════════════════════════════════════════════

describe('simple path — logical operators', () => {
  it('_and combines conditions', async () => {
    const filter = {
      _and: [{ category: { _eq: 'electronics' } }, { price: { _gt: 500 } }],
    };
    const p = plan('products', { fields: 'id,name,price,category', filter });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name,price,category',
      filter,
      metadata,
      plan: p,
    });
    expect(
      r.data.every(
        (row: any) => row.category === 'electronics' && row.price > 500,
      ),
    ).toBe(true);
  });

  it('_or matches either condition', async () => {
    const filter = {
      _or: [{ category: { _eq: 'food' } }, { price: { _lt: 30 } }],
    };
    const p = plan('products', { fields: 'id,price,category', filter });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price,category',
      filter,
      metadata,
      plan: p,
    });
    expect(
      r.data.every((row: any) => row.category === 'food' || row.price < 30),
    ).toBe(true);
  });

  it('_not negates condition', async () => {
    const filter = { _not: { category: { _eq: 'electronics' } } };
    const p = plan('products', { fields: 'id,category', filter });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,category',
      filter,
      metadata,
      plan: p,
    });
    expect(r.data.every((row: any) => row.category !== 'electronics')).toBe(
      true,
    );
  });

  it('nested _and inside _or', async () => {
    const filter = {
      _or: [
        {
          _and: [{ category: { _eq: 'electronics' } }, { price: { _gt: 500 } }],
        },
        { _and: [{ category: { _eq: 'books' } }, { price: { _lt: 100 } }] },
      ],
    };
    const p = plan('products', { fields: 'id,price,category', filter });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price,category',
      filter,
      metadata,
      plan: p,
    });
    expect(
      r.data.every(
        (row: any) =>
          (row.category === 'electronics' && row.price > 500) ||
          (row.category === 'books' && row.price < 100),
      ),
    ).toBe(true);
  });

  it('deep nesting: _not inside _or inside _and', async () => {
    const filter = {
      _and: [
        { price: { _gte: 100 } },
        {
          _or: [
            { category: { _eq: 'electronics' } },
            { _not: { active: { _eq: false } } },
          ],
        },
      ],
    };
    const p = plan('products', { fields: 'id,price,category,active', filter });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price,category,active',
      filter,
      metadata,
      plan: p,
    });
    expect(
      r.data.every(
        (row: any) =>
          row.price >= 100 &&
          (row.category === 'electronics' || row.active !== false),
      ),
    ).toBe(true);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Sort
// ═══════════════════════════════════════════════════════════════════

describe('simple path — sort', () => {
  it('sort ascending', async () => {
    const p = plan('products', { fields: 'id,price', sort: 'price', limit: 5 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      sort: 'price',
      limit: 5,
      metadata,
      plan: p,
    });
    const prices = r.data.map((row: any) => row.price);
    for (let i = 1; i < prices.length; i++) {
      expect(prices[i]).toBeGreaterThanOrEqual(prices[i - 1]);
    }
  });

  it('sort descending', async () => {
    const p = plan('products', {
      fields: 'id,price',
      sort: '-price',
      limit: 5,
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      sort: '-price',
      limit: 5,
      metadata,
      plan: p,
    });
    const prices = r.data.map((row: any) => row.price);
    for (let i = 1; i < prices.length; i++) {
      expect(prices[i]).toBeLessThanOrEqual(prices[i - 1]);
    }
  });

  it('sort by id ascending by default', async () => {
    const p = plan('products', { fields: 'id', limit: 5 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 5,
      metadata,
      plan: p,
    });
    const ids = r.data.map((row: any) => row.id);
    for (let i = 1; i < ids.length; i++) {
      expect(ids[i]).toBeGreaterThan(ids[i - 1]);
    }
  });
});

// ═══════════════════════════════════════════════════════════════════
// Pagination
// ═══════════════════════════════════════════════════════════════════

describe('simple path — pagination', () => {
  it('limit restricts results', async () => {
    const p = plan('products', { fields: 'id', limit: 10 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 10,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(10);
  });

  it('limit=1 returns single row', async () => {
    const p = plan('products', { fields: 'id', limit: 1 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 1,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(1);
  });

  it('page+limit paginates correctly', async () => {
    const p1 = plan('products', {
      fields: 'id',
      sort: 'id',
      limit: 5,
      page: 1,
    });
    const p2 = plan('products', {
      fields: 'id',
      sort: 'id',
      limit: 5,
      page: 2,
    });
    const r1 = await executor().execute({
      tableName: 'products',
      fields: 'id',
      sort: 'id',
      limit: 5,
      page: 1,
      metadata,
      plan: p1,
    });
    const r2 = await executor().execute({
      tableName: 'products',
      fields: 'id',
      sort: 'id',
      limit: 5,
      page: 2,
      metadata,
      plan: p2,
    });
    expect(r1.data).toHaveLength(5);
    expect(r2.data).toHaveLength(5);
    expect(r1.data[0].id).toBeLessThan(r2.data[0].id);
  });

  it('page beyond data returns empty', async () => {
    const p = plan('products', { fields: 'id', limit: 10, page: 100 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 10,
      page: 100,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(0);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Field selection
// ═══════════════════════════════════════════════════════════════════

describe('simple path — field selection', () => {
  it('single field', async () => {
    const p = plan('products', { fields: 'name', limit: 1 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'name',
      limit: 1,
      metadata,
      plan: p,
    });
    expect(Object.keys(r.data[0])).toEqual(['name']);
  });

  it('multiple fields', async () => {
    const p = plan('products', { fields: 'id,name,price', limit: 1 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name,price',
      limit: 1,
      metadata,
      plan: p,
    });
    expect(Object.keys(r.data[0]).sort()).toEqual(['id', 'name', 'price']);
  });

  it('all fields', async () => {
    const p = plan('products', {
      fields: 'id,name,price,active,category,rating,tags,createdAt',
      limit: 1,
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name,price,active,category,rating,tags,createdAt',
      limit: 1,
      metadata,
      plan: p,
    });
    expect(Object.keys(r.data[0]).length).toBe(8);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Meta
// ═══════════════════════════════════════════════════════════════════

describe('simple path — meta', () => {
  it('totalCount', async () => {
    const p = plan('products', { fields: 'id', limit: 5, meta: 'totalCount' });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 5,
      meta: 'totalCount',
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(5);
    expect(r.meta.totalCount).toBe(100);
  });

  it('filterCount', async () => {
    const filter = { category: { _eq: 'electronics' } };
    const p = plan('products', {
      fields: 'id',
      limit: 5,
      meta: 'filterCount',
      filter,
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 5,
      meta: 'filterCount',
      filter,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(5);
    expect(r.meta.filterCount).toBe(20);
  });

  it('both totalCount and filterCount', async () => {
    const filter = { category: { _eq: 'books' } };
    const p = plan('products', {
      fields: 'id',
      limit: 3,
      meta: 'totalCount,filterCount',
      filter,
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 3,
      meta: 'totalCount,filterCount',
      filter,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(3);
    expect(r.meta.totalCount).toBe(100);
    expect(r.meta.filterCount).toBe(20);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Empty table
// ═══════════════════════════════════════════════════════════════════

describe('simple path — empty table', () => {
  it('returns empty array', async () => {
    const p = plan('empty_table', { fields: 'id,value' });
    const r = await executor().execute({
      tableName: 'empty_table',
      fields: 'id,value',
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(0);
  });

  it('totalCount on empty table is 0', async () => {
    const p = plan('empty_table', { fields: 'id', meta: 'totalCount' });
    const r = await executor().execute({
      tableName: 'empty_table',
      fields: 'id',
      meta: 'totalCount',
      metadata,
      plan: p,
    });
    expect(r.meta.totalCount).toBe(0);
  });

  it('filter with no matches on empty table', async () => {
    const p = plan('empty_table', {
      fields: 'id',
      filter: { value: { _eq: 'x' } },
      meta: 'filterCount',
    });
    const r = await executor().execute({
      tableName: 'empty_table',
      fields: 'id',
      filter: { value: { _eq: 'x' } },
      meta: 'filterCount',
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(0);
    expect(r.meta.filterCount).toBe(0);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Combined filter + sort + pagination + meta
// ═══════════════════════════════════════════════════════════════════

describe('simple path — combined operations', () => {
  it('filter + sort + limit + meta', async () => {
    const filter = { active: { _eq: true }, price: { _gte: 200 } };
    const p = plan('products', {
      fields: 'id,name,price',
      filter,
      sort: '-price',
      limit: 5,
      meta: 'filterCount',
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name,price',
      filter,
      sort: '-price',
      limit: 5,
      meta: 'filterCount',
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(5);
    expect(r.meta.filterCount).toBeGreaterThan(0);
    const prices = r.data.map((row: any) => row.price);
    for (let i = 1; i < prices.length; i++) {
      expect(prices[i]).toBeLessThanOrEqual(prices[i - 1]);
    }
    expect(r.data.every((row: any) => row.price >= 200)).toBe(true);
  });

  it('filter with _and + pagination', async () => {
    const filter = {
      _and: [
        { category: { _in: ['electronics', 'books'] } },
        { rating: { _gte: 3 } },
      ],
    };
    const p1 = plan('products', {
      fields: 'id,category,rating',
      filter,
      sort: 'id',
      limit: 5,
      page: 1,
    });
    const p2 = plan('products', {
      fields: 'id,category,rating',
      filter,
      sort: 'id',
      limit: 5,
      page: 2,
    });
    const r1 = await executor().execute({
      tableName: 'products',
      fields: 'id,category,rating',
      filter,
      sort: 'id',
      limit: 5,
      page: 1,
      metadata,
      plan: p1,
    });
    const r2 = await executor().execute({
      tableName: 'products',
      fields: 'id,category,rating',
      filter,
      sort: 'id',
      limit: 5,
      page: 2,
      metadata,
      plan: p2,
    });
    expect(r1.data).toHaveLength(5);
    expect(r2.data).toHaveLength(5);
    expect(r1.data[r1.data.length - 1].id).toBeLessThan(r2.data[0].id);
  });

  it('complex nested filter + sort desc + meta', async () => {
    const filter = {
      _or: [
        { _and: [{ category: { _eq: 'toys' } }, { price: { _lte: 500 } }] },
        { _and: [{ category: { _eq: 'food' } }, { rating: { _gte: 4 } }] },
      ],
    };
    const p = plan('products', {
      fields: 'id,category,price,rating',
      filter,
      sort: '-price',
      limit: 10,
      meta: 'filterCount,totalCount',
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,category,price,rating',
      filter,
      sort: '-price',
      limit: 10,
      meta: 'filterCount,totalCount',
      metadata,
      plan: p,
    });
    expect(r.data.length).toBeGreaterThan(0);
    expect(r.data.length).toBeLessThanOrEqual(10);
    expect(r.meta.totalCount).toBe(100);
    expect(r.meta.filterCount).toBeGreaterThan(0);
    expect(
      r.data.every(
        (row: any) =>
          (row.category === 'toys' && row.price <= 500) ||
          (row.category === 'food' && row.rating >= 4),
      ),
    ).toBe(true);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Edge cases
// ═══════════════════════════════════════════════════════════════════

describe('simple path — edge cases', () => {
  it('filter with multiple conditions on same field', async () => {
    const filter = {
      _and: [{ price: { _gte: 100 } }, { price: { _lte: 200 } }],
    };
    const p = plan('products', { fields: 'id,price', filter });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,price',
      filter,
      metadata,
      plan: p,
    });
    expect(
      r.data.every((row: any) => row.price >= 100 && row.price <= 200),
    ).toBe(true);
    expect(r.data.length).toBe(11);
  });

  it('no filter, no sort, just limit', async () => {
    const p = plan('products', { fields: 'id', limit: 3 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      limit: 3,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(3);
  });

  it('filter that matches everything', async () => {
    const filter = { price: { _gte: 0 } };
    const p = plan('products', { fields: 'id', filter, limit: 200 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      filter,
      limit: 200,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(100);
  });

  it('filter with _eq on boolean true', async () => {
    const p = plan('products', {
      fields: 'id,active',
      filter: { active: { _eq: true } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,active',
      filter: { active: { _eq: true } },
      metadata,
      plan: p,
    });
    expect(
      r.data.every((row: any) => row.active === 1 || row.active === true),
    ).toBe(true);
  });

  it('filter with _eq on boolean false', async () => {
    const p = plan('products', {
      fields: 'id,active',
      filter: { active: { _eq: false } },
    });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,active',
      filter: { active: { _eq: false } },
      metadata,
      plan: p,
    });
    expect(
      r.data.every((row: any) => row.active === 0 || row.active === false),
    ).toBe(true);
  });

  it('implicit _eq shorthand (filter: { field: value })', async () => {
    const p = plan('products', { fields: 'id,name', filter: { id: 1 } });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id,name',
      filter: { id: 1 },
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(1);
    expect(r.data[0].id).toBe(1);
  });

  it('empty filter object returns all', async () => {
    const p = plan('products', { fields: 'id', filter: {}, limit: 200 });
    const r = await executor().execute({
      tableName: 'products',
      fields: 'id',
      filter: {},
      limit: 200,
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(100);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Full path fallback — verify still works for relation tables
// ═══════════════════════════════════════════════════════════════════

describe('full path fallback — orders table (has relations)', () => {
  it('basic query on table with relations', async () => {
    const p = plan('orders', { fields: 'id,total,status' });
    const r = await executor().execute({
      tableName: 'orders',
      fields: 'id,total,status',
      metadata,
      plan: p,
    });
    expect(r.data).toHaveLength(50);
  });

  it('filter + sort on table with relations', async () => {
    const filter = { status: { _eq: 'pending' } };
    const p = plan('orders', {
      fields: 'id,total,status',
      filter,
      sort: '-total',
    });
    const r = await executor().execute({
      tableName: 'orders',
      fields: 'id,total,status',
      filter,
      sort: '-total',
      metadata,
      plan: p,
    });
    expect(r.data.every((row: any) => row.status === 'pending')).toBe(true);
    const totals = r.data.map((row: any) => row.total);
    for (let i = 1; i < totals.length; i++) {
      expect(totals[i]).toBeLessThanOrEqual(totals[i - 1]);
    }
  });

  it('pagination on table with relations', async () => {
    const p1 = plan('orders', { fields: 'id', sort: 'id', limit: 10, page: 1 });
    const p2 = plan('orders', { fields: 'id', sort: 'id', limit: 10, page: 2 });
    const r1 = await executor().execute({
      tableName: 'orders',
      fields: 'id',
      sort: 'id',
      limit: 10,
      page: 1,
      metadata,
      plan: p1,
    });
    const r2 = await executor().execute({
      tableName: 'orders',
      fields: 'id',
      sort: 'id',
      limit: 10,
      page: 2,
      metadata,
      plan: p2,
    });
    expect(r1.data).toHaveLength(10);
    expect(r2.data).toHaveLength(10);
    expect(r1.data[9].id).toBeLessThan(r2.data[0].id);
  });
});

// ═══════════════════════════════════════════════════════════════════
// Cross-path parity: simple path results must equal full path results
// ═══════════════════════════════════════════════════════════════════

describe('cross-path parity — simple vs full path produce same data', () => {
  it('same filter+sort+limit returns identical rows', async () => {
    const filter = { category: { _eq: 'electronics' }, price: { _gte: 300 } };
    const fields = 'id,name,price,category';

    const pSimple = plan('products', {
      fields,
      filter,
      sort: '-price',
      limit: 5,
    });
    const rSimple = await executor().execute({
      tableName: 'products',
      fields,
      filter,
      sort: '-price',
      limit: 5,
      metadata,
      plan: pSimple,
    });

    const exec2 = new SqlQueryExecutor(db, 'sqlite');
    const rFull = await exec2.execute({
      tableName: 'products',
      fields,
      filter,
      sort: '-price',
      limit: 5,
      metadata,
    });

    expect(rSimple.data).toHaveLength(rFull.data.length);
    for (let i = 0; i < rSimple.data.length; i++) {
      expect(rSimple.data[i].id).toBe(rFull.data[i].id);
      expect(rSimple.data[i].price).toBe(rFull.data[i].price);
    }
  });

  it('same pagination produces consistent pages', async () => {
    const fields = 'id,name';
    const limit = 10;

    for (let page = 1; page <= 3; page++) {
      const pSimple = plan('products', { fields, sort: 'id', limit, page });
      const rSimple = await executor().execute({
        tableName: 'products',
        fields,
        sort: 'id',
        limit,
        page,
        metadata,
        plan: pSimple,
      });

      const exec2 = new SqlQueryExecutor(db, 'sqlite');
      const rFull = await exec2.execute({
        tableName: 'products',
        fields,
        sort: 'id',
        limit,
        page,
        metadata,
      });

      expect(rSimple.data).toHaveLength(rFull.data.length);
      for (let i = 0; i < rSimple.data.length; i++) {
        expect(rSimple.data[i].id).toBe(rFull.data[i].id);
      }
    }
  });
});
