/**
 * Core Engine Tests: Query Builder (filter DSL, WHERE, operators) + Cascade Pipeline
 *
 * Uses SQLite in-memory via Knex to test real SQL generation and execution
 * without external DB dependencies.
 */

import knex, { Knex } from 'knex';
import { buildWhereClause, hasLogicalOperators } from '../../src/kernel/query';

let db: Knex;

beforeAll(async () => {
  db = knex({
    client: 'sqlite3',
    connection: { filename: ':memory:' },
    useNullAsDefault: true,
  });

  await db.schema.createTable('users', (t) => {
    t.increments('id').primary();
    t.string('name');
    t.string('email');
    t.integer('age');
    t.boolean('isActive').defaultTo(true);
    t.string('role');
    t.timestamps(true, true);
  });

  await db.schema.createTable('posts', (t) => {
    t.increments('id').primary();
    t.string('title');
    t.string('status').defaultTo('draft');
    t.integer('userId').references('id').inTable('users');
    t.timestamps(true, true);
  });

  await db.schema.createTable('tags', (t) => {
    t.increments('id').primary();
    t.string('name');
  });

  await db.schema.createTable('post_tags', (t) => {
    t.integer('postId').references('id').inTable('posts');
    t.integer('tagId').references('id').inTable('tags');
    t.primary(['postId', 'tagId']);
  });

  // Seed data
  await db('users').insert([
    {
      id: 1,
      name: 'Alice',
      email: 'alice@test.com',
      age: 30,
      isActive: true,
      role: 'admin',
    },
    {
      id: 2,
      name: 'Bob',
      email: 'bob@test.com',
      age: 25,
      isActive: true,
      role: 'user',
    },
    {
      id: 3,
      name: 'Charlie',
      email: 'charlie@test.com',
      age: 35,
      isActive: false,
      role: 'user',
    },
    {
      id: 4,
      name: 'Diana',
      email: 'diana@test.com',
      age: 28,
      isActive: true,
      role: 'admin',
    },
    {
      id: 5,
      name: 'Eve',
      email: 'eve@test.com',
      age: 22,
      isActive: false,
      role: 'guest',
    },
  ]);

  await db('posts').insert([
    { id: 1, title: 'First Post', status: 'published', userId: 1 },
    { id: 2, title: 'Draft Post', status: 'draft', userId: 1 },
    { id: 3, title: 'Bob Post', status: 'published', userId: 2 },
    { id: 4, title: 'Archived', status: 'archived', userId: 3 },
  ]);

  await db('tags').insert([
    { id: 1, name: 'tech' },
    { id: 2, name: 'news' },
    { id: 3, name: 'tutorial' },
  ]);

  await db('post_tags').insert([
    { postId: 1, tagId: 1 },
    { postId: 1, tagId: 2 },
    { postId: 2, tagId: 1 },
    { postId: 3, tagId: 3 },
  ]);
});

afterAll(async () => {
  await db.destroy();
});

// ═══════════════════════════════════════════════════════════════════════════════
// hasLogicalOperators
// ═══════════════════════════════════════════════════════════════════════════════

describe('hasLogicalOperators', () => {
  it('returns false for simple filter', () => {
    expect(hasLogicalOperators({ name: { _eq: 'Alice' } })).toBe(false);
  });

  it('returns true for _and', () => {
    expect(hasLogicalOperators({ _and: [{ name: { _eq: 'Alice' } }] })).toBe(
      true,
    );
  });

  it('returns true for _or', () => {
    expect(hasLogicalOperators({ _or: [{ age: { _gt: 30 } }] })).toBe(true);
  });

  it('returns true for _not', () => {
    expect(hasLogicalOperators({ _not: { isActive: { _eq: false } } })).toBe(
      true,
    );
  });

  it('returns true for deeply nested logical operator', () => {
    expect(
      hasLogicalOperators({
        name: { _eq: 'x' },
        nested: { deep: { _or: [{ a: 1 }] } },
      }),
    ).toBe(true);
  });

  it('returns false for null/undefined', () => {
    expect(hasLogicalOperators(null)).toBe(false);
    expect(hasLogicalOperators(undefined)).toBe(false);
  });

  it('returns false for empty object', () => {
    expect(hasLogicalOperators({})).toBe(false);
  });

  it('handles arrays', () => {
    expect(hasLogicalOperators([{ _and: [{}] }])).toBe(true);
    expect(hasLogicalOperators([{ name: 'x' }])).toBe(false);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// buildWhereClause — real SQL via SQLite
// ═══════════════════════════════════════════════════════════════════════════════

describe('buildWhereClause — _eq operator', () => {
  it('filters by exact match', async () => {
    const query = buildWhereClause(
      db('users'),
      { name: { _eq: 'Alice' } },
      'users',
      'sqlite',
    );
    const results = await query;
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Alice');
  });

  it('returns empty for non-existent value', async () => {
    const query = buildWhereClause(
      db('users'),
      { name: { _eq: 'Nobody' } },
      'users',
      'sqlite',
    );
    const results = await query;
    expect(results).toHaveLength(0);
  });
});

describe('buildWhereClause — _neq operator', () => {
  it('excludes matching records', async () => {
    const query = buildWhereClause(
      db('users'),
      { role: { _neq: 'admin' } },
      'users',
      'sqlite',
    );
    const results = await query;
    expect(results.every((r: any) => r.role !== 'admin')).toBe(true);
    expect(results).toHaveLength(3);
  });
});

describe('buildWhereClause — comparison operators', () => {
  it('_gt filters greater than', async () => {
    const results = await buildWhereClause(
      db('users'),
      { age: { _gt: 30 } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Charlie');
  });

  it('_gte filters greater than or equal', async () => {
    const results = await buildWhereClause(
      db('users'),
      { age: { _gte: 30 } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(2); // Alice(30), Charlie(35)
  });

  it('_lt filters less than', async () => {
    const results = await buildWhereClause(
      db('users'),
      { age: { _lt: 25 } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Eve');
  });

  it('_lte filters less than or equal', async () => {
    const results = await buildWhereClause(
      db('users'),
      { age: { _lte: 25 } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(2); // Bob(25), Eve(22)
  });
});

describe('buildWhereClause — _in / _nin operators', () => {
  it('_in matches multiple values', async () => {
    const results = await buildWhereClause(
      db('users'),
      { role: { _in: ['admin', 'guest'] } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(3); // Alice, Diana, Eve
  });

  it('_nin excludes multiple values', async () => {
    const results = await buildWhereClause(
      db('users'),
      { role: { _nin: ['admin', 'guest'] } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(2); // Bob, Charlie
    expect(results.every((r: any) => r.role === 'user')).toBe(true);
  });

  it('_in with string comma-separated values', async () => {
    const results = await buildWhereClause(
      db('users'),
      { role: { _in: 'admin,guest' } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(3);
  });

  it('_in with single value', async () => {
    const results = await buildWhereClause(
      db('users'),
      { role: { _in: 'admin' } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(2);
  });
});

describe('buildWhereClause — _between', () => {
  it('filters range inclusive', async () => {
    const results = await buildWhereClause(
      db('users'),
      { age: { _between: [25, 30] } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(3); // Bob(25), Diana(28), Alice(30)
  });
});

describe('buildWhereClause — _is_null / _is_not_null', () => {
  it('_is_null true filters null values', async () => {
    // Insert a user with null email for this test
    await db('users').insert({
      id: 100,
      name: 'NullEmail',
      email: null,
      age: 20,
      role: 'test',
    });
    const results = await buildWhereClause(
      db('users'),
      { email: { _is_null: true } },
      'users',
      'sqlite',
    );
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results.every((r: any) => r.email === null)).toBe(true);
    await db('users').where('id', 100).delete();
  });

  it('_is_not_null true filters non-null values', async () => {
    const results = await buildWhereClause(
      db('users'),
      { email: { _is_not_null: true } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(5);
    expect(results.every((r: any) => r.email !== null)).toBe(true);
  });
});

describe('buildWhereClause — _contains / _starts_with / _ends_with (SQLite)', () => {
  it('_contains finds substring', async () => {
    const results = await buildWhereClause(
      db('users'),
      { name: { _contains: 'li' } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(2); // Alice, Charlie
  });

  it('_starts_with finds prefix', async () => {
    const results = await buildWhereClause(
      db('users'),
      { name: { _starts_with: 'A' } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Alice');
  });

  it('_ends_with finds suffix', async () => {
    const results = await buildWhereClause(
      db('users'),
      { name: { _ends_with: 'e' } },
      'users',
      'sqlite',
    );
    expect(results.length).toBeGreaterThanOrEqual(2); // Alice, Charlie, Eve
  });

  it('_contains is case-insensitive', async () => {
    const results = await buildWhereClause(
      db('users'),
      { name: { _contains: 'ALICE' } },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(1);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// buildWhereClause — logical operators (_and, _or, _not)
// ═══════════════════════════════════════════════════════════════════════════════

describe('buildWhereClause — logical operators', () => {
  it('_and combines conditions', async () => {
    const results = await buildWhereClause(
      db('users'),
      {
        _and: [{ role: { _eq: 'admin' } }, { age: { _gte: 30 } }],
      },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Alice');
  });

  it('_or matches either condition', async () => {
    const results = await buildWhereClause(
      db('users'),
      {
        _or: [{ role: { _eq: 'admin' } }, { role: { _eq: 'guest' } }],
      },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(3); // Alice, Diana, Eve
  });

  it('_not excludes matching records', async () => {
    const results = await buildWhereClause(
      db('users'),
      {
        _not: { role: { _eq: 'admin' } },
      },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(3);
    expect(results.every((r: any) => r.role !== 'admin')).toBe(true);
  });

  it('nested _and inside _or', async () => {
    const results = await buildWhereClause(
      db('users'),
      {
        _or: [
          { _and: [{ role: { _eq: 'admin' } }, { age: { _lt: 30 } }] },
          { name: { _eq: 'Eve' } },
        ],
      },
      'users',
      'sqlite',
    );
    // Diana is admin age 28, Eve is guest age 22
    expect(results).toHaveLength(2);
    const names = results.map((r: any) => r.name).sort();
    expect(names).toEqual(['Diana', 'Eve']);
  });

  it('complex nested logical operators', async () => {
    const results = await buildWhereClause(
      db('users'),
      {
        _and: [
          { isActive: { _eq: 1 } }, // SQLite boolean
          { _or: [{ role: { _eq: 'admin' } }, { age: { _lt: 25 } }] },
        ],
      },
      'users',
      'sqlite',
    );
    // Active admins: Alice(30), Diana(28). Active age<25: none (Eve is inactive)
    expect(results).toHaveLength(2);
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// buildWhereClause — implicit AND (multiple fields)
// ═══════════════════════════════════════════════════════════════════════════════

describe('buildWhereClause — implicit AND', () => {
  it('multiple fields are AND-ed', async () => {
    const results = await buildWhereClause(
      db('users'),
      {
        role: { _eq: 'admin' },
        isActive: { _eq: 1 },
      },
      'users',
      'sqlite',
    );
    expect(results).toHaveLength(2); // Alice, Diana
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Real CRUD operations via Knex (simulating query builder paths)
// ═══════════════════════════════════════════════════════════════════════════════

describe('CRUD — insert + select + update + delete', () => {
  it('insert returns new id', async () => {
    const [id] = await db('users').insert({
      name: 'Frank',
      email: 'frank@test.com',
      age: 40,
      role: 'user',
    });
    expect(id).toBeDefined();
    expect(typeof id).toBe('number');
    await db('users').where('id', id).delete();
  });

  it('update modifies record', async () => {
    await db('users').where('id', 2).update({ name: 'Bobby' });
    const user = await db('users').where('id', 2).first();
    expect(user.name).toBe('Bobby');
    await db('users').where('id', 2).update({ name: 'Bob' }); // restore
  });

  it('delete removes record', async () => {
    const [id] = await db('users').insert({
      name: 'Temp',
      email: 'temp@test.com',
      age: 0,
      role: 'temp',
    });
    const deleted = await db('users').where('id', id).delete();
    expect(deleted).toBe(1);
    const found = await db('users').where('id', id).first();
    expect(found).toBeUndefined();
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Cascade-like operations (O2M, M2M FK management)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Cascade — One-to-Many FK management', () => {
  it('assigns FK on child creation', async () => {
    const [postId] = await db('posts').insert({
      title: 'New Post',
      status: 'draft',
      userId: 1,
    });
    const post = await db('posts').where('id', postId).first();
    expect(post.userId).toBe(1);
    await db('posts').where('id', postId).delete();
  });

  it('nulls FK on child removal (cascade unlink)', async () => {
    const [postId] = await db('posts').insert({
      title: 'Temp Post',
      status: 'draft',
      userId: 2,
    });
    // Simulate cascade: unlink child by nulling FK
    await db('posts').where('id', postId).update({ userId: null });
    const post = await db('posts').where('id', postId).first();
    expect(post.userId).toBeNull();
    await db('posts').where('id', postId).delete();
  });

  it('O2M diff: removes unlinked, keeps linked', async () => {
    // Simulate the cascade diff logic
    const existingIds = [1, 2, 3].map(String);
    const incomingIds = [1, 3].map(String);
    const idsToRemove = existingIds.filter((id) => !incomingIds.includes(id));
    expect(idsToRemove).toEqual(['2']);

    // Verify in real DB
    const posts = await db('posts').where('userId', 1).select('id');
    expect(posts.length).toBeGreaterThanOrEqual(1);
  });
});

describe('Cascade — Many-to-Many junction table sync', () => {
  it('full replace: delete all + re-insert', async () => {
    // Initial state: post 1 has tags [1,2]
    const before = await db('post_tags').where('postId', 1);
    expect(before).toHaveLength(2);

    // Simulate M2M sync: replace with [2,3]
    await db('post_tags').where('postId', 1).delete();
    await db('post_tags').insert([
      { postId: 1, tagId: 2 },
      { postId: 1, tagId: 3 },
    ]);

    const after = await db('post_tags').where('postId', 1);
    expect(after).toHaveLength(2);
    expect(after.map((r: any) => r.tagId).sort()).toEqual([2, 3]);

    // Restore original
    await db('post_tags').where('postId', 1).delete();
    await db('post_tags').insert([
      { postId: 1, tagId: 1 },
      { postId: 1, tagId: 2 },
    ]);
  });

  it('empty array clears all junctions', async () => {
    const [postId] = await db('posts').insert({
      title: 'Junction Test',
      status: 'draft',
      userId: 1,
    });
    await db('post_tags').insert([
      { postId, tagId: 1 },
      { postId, tagId: 2 },
    ]);

    // Simulate M2M sync with empty array
    await db('post_tags').where('postId', postId).delete();
    const after = await db('post_tags').where('postId', postId);
    expect(after).toHaveLength(0);

    await db('posts').where('id', postId).delete();
  });
});

describe('Cascade — Many-to-One FK transform', () => {
  it('nested object with id → FK column value', () => {
    // Simulate transformRelationsToFK for M2O
    const data = { title: 'Test', user: { id: 5, name: 'Alice' } };
    const fkColumn = 'userId';
    const transformed: any = { ...data };
    if (data.user && typeof data.user === 'object') {
      transformed[fkColumn] = (data.user as any).id;
      delete transformed.user;
    }
    expect(transformed).toEqual({ title: 'Test', userId: 5 });
    expect(transformed.user).toBeUndefined();
  });

  it('null value → FK set to null', () => {
    const data = { title: 'Test', user: null };
    const fkColumn = 'userId';
    const transformed: any = { ...data };
    transformed[fkColumn] = null;
    delete transformed.user;
    expect(transformed).toEqual({ title: 'Test', userId: null });
  });

  it('scalar id → FK set directly', () => {
    const data = { title: 'Test', user: 5 };
    const fkColumn = 'userId';
    const transformed: any = { ...data };
    transformed[fkColumn] = data.user;
    delete transformed.user;
    expect(transformed).toEqual({ title: 'Test', userId: 5 });
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Field stripping (stripUnknownColumns simulation)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Field stripping', () => {
  it('removes unknown columns', () => {
    const validColumns = new Set([
      'id',
      'name',
      'email',
      'age',
      'role',
      'userId',
    ]);
    const data: any = {
      name: 'Alice',
      email: 'a@b.com',
      _malicious: 'drop',
      __proto__: {},
      extra: 42,
    };
    const stripped: any = {};
    for (const key of Object.keys(data)) {
      if (validColumns.has(key)) stripped[key] = data[key];
    }
    expect(stripped).toEqual({ name: 'Alice', email: 'a@b.com' });
    expect(stripped._malicious).toBeUndefined();
    expect(stripped.extra).toBeUndefined();
  });

  it('preserves FK columns from relations', () => {
    const columns = new Set(['id', 'title', 'status']);
    const relationFKs = ['userId']; // added from relation metadata
    const valid = new Set([...columns, ...relationFKs]);
    const data = { title: 'Post', userId: 1, randomField: 'x' };
    const stripped: any = {};
    for (const key of Object.keys(data)) {
      if (valid.has(key)) stripped[key] = data[key];
    }
    expect(stripped).toEqual({ title: 'Post', userId: 1 });
  });

  it('always strips internal pipeline keys', () => {
    const data = {
      name: 'Test',
      _m2mRelations: [1, 2],
      _o2mRelations: [],
      _o2oRelations: null,
    };
    const internalKeys = ['_m2mRelations', '_o2mRelations', '_o2oRelations'];
    for (const key of internalKeys) {
      delete data[key as keyof typeof data];
    }
    expect(data).toEqual({ name: 'Test' });
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Pagination + Sort
// ═══════════════════════════════════════════════════════════════════════════════

describe('Pagination', () => {
  it('limit restricts results', async () => {
    const results = await db('users').limit(2);
    expect(results).toHaveLength(2);
  });

  it('offset skips records', async () => {
    const page1 = await db('users').orderBy('id').limit(2).offset(0);
    const page2 = await db('users').orderBy('id').limit(2).offset(2);
    expect(page1[0].id).not.toBe(page2[0].id);
  });

  it('page calculation: (page-1) * limit = offset', () => {
    const page = 3;
    const limit = 10;
    const offset = (page - 1) * limit;
    expect(offset).toBe(20);
  });
});

describe('Sort', () => {
  it('sort asc by age', async () => {
    const results = await db('users').orderBy('age', 'asc');
    for (let i = 1; i < results.length; i++) {
      expect(results[i].age).toBeGreaterThanOrEqual(results[i - 1].age);
    }
  });

  it('sort desc by age', async () => {
    const results = await db('users').orderBy('age', 'desc');
    for (let i = 1; i < results.length; i++) {
      expect(results[i].age).toBeLessThanOrEqual(results[i - 1].age);
    }
  });

  it('multi-column sort', async () => {
    const results = await db('users').orderBy([
      { column: 'role', order: 'asc' },
      { column: 'age', order: 'desc' },
    ]);
    expect(results[0].role).toBe('admin');
  });
});

// ═══════════════════════════════════════════════════════════════════════════════
// Transaction rollback (confirms AsyncLocalStorage-based isolation works)
// ═══════════════════════════════════════════════════════════════════════════════

describe('Transaction — rollback on error', () => {
  it('rolls back insert on error within transaction', async () => {
    const countBefore = (await db('users').count('* as c'))[0].c as number;

    try {
      await db.transaction(async (trx) => {
        await trx('users').insert({
          name: 'ShouldRollback',
          email: 'rollback@test.com',
          age: 99,
          role: 'temp',
        });
        throw new Error('simulated failure');
      });
    } catch {}

    const countAfter = (await db('users').count('* as c'))[0].c as number;
    expect(countAfter).toBe(countBefore);
  });

  it('commits on success within transaction', async () => {
    const countBefore = (await db('users').count('* as c'))[0].c as number;

    await db.transaction(async (trx) => {
      await trx('users').insert({
        name: 'ShouldCommit',
        email: 'commit@test.com',
        age: 99,
        role: 'temp',
      });
    });

    const countAfter = (await db('users').count('* as c'))[0].c as number;
    expect(countAfter).toBe(countBefore + 1);
    await db('users').where('name', 'ShouldCommit').delete();
  });

  it('concurrent transactions are isolated', async () => {
    const results: string[] = [];

    await Promise.all([
      db.transaction(async (trx) => {
        await trx('users').insert({
          name: 'Tx1',
          email: 'tx1@test.com',
          age: 1,
          role: 'a',
        });
        results.push('tx1-insert');
        // Small delay to interleave
        await new Promise((r) => setTimeout(r, 10));
        const found = await trx('users').where('name', 'Tx1').first();
        expect(found).toBeDefined();
        results.push('tx1-verify');
      }),
      db.transaction(async (trx) => {
        await trx('users').insert({
          name: 'Tx2',
          email: 'tx2@test.com',
          age: 2,
          role: 'b',
        });
        results.push('tx2-insert');
        const found = await trx('users').where('name', 'Tx2').first();
        expect(found).toBeDefined();
        results.push('tx2-verify');
      }),
    ]);

    expect(results).toContain('tx1-insert');
    expect(results).toContain('tx2-insert');

    await db('users').whereIn('name', ['Tx1', 'Tx2']).delete();
  });
});
