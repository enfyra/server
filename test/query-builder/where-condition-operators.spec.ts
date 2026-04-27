import { describe, it, expect } from 'vitest';
import { whereToMongoFilter, applyWhereToKnex } from 'src/kernel/query';
import type { WhereCondition } from '../../src/shared/types/query-builder.types';

// All 16 operators from WhereOperator union — must cover every one.
const ALL_OPERATORS = [
  '=',
  '!=',
  '>',
  '<',
  '>=',
  '<=',
  'like',
  'in',
  'not in',
  'is null',
  'is not null',
  '_contains',
  '_starts_with',
  '_ends_with',
  '_between',
  '_is_null',
  '_is_not_null',
] as const;

const emptyMetadata = { tables: new Map() };

function makeKnexStub() {
  const calls: Array<{ method: string; args: any[] }> = [];
  const handler: ProxyHandler<any> = {
    get(_t, prop: string) {
      return (...args: any[]) => {
        calls.push({ method: prop, args });
        return new Proxy({}, handler);
      };
    },
  };
  const stub: any = new Proxy({}, handler);
  return { stub, calls };
}

describe('whereToMongoFilter — every operator covered', () => {
  it('= → direct equality', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'name', operator: '=', value: 'alice' },
    ]);
    expect(f).toEqual({ name: 'alice' });
  });

  it('!= → $ne', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'name', operator: '!=', value: 'alice' },
    ]);
    expect(f).toEqual({ name: { $ne: 'alice' } });
  });

  it('> → $gt', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'age', operator: '>', value: 18 },
    ]);
    expect(f).toEqual({ age: { $gt: 18 } });
  });

  it('< → $lt', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'age', operator: '<', value: 18 },
    ]);
    expect(f).toEqual({ age: { $lt: 18 } });
  });

  it('>= → $gte', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'age', operator: '>=', value: 18 },
    ]);
    expect(f).toEqual({ age: { $gte: 18 } });
  });

  it('<= → $lte', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'age', operator: '<=', value: 18 },
    ]);
    expect(f).toEqual({ age: { $lte: 18 } });
  });

  it('like → regex with .* and case-insensitive', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'name', operator: 'like', value: 'al%' },
    ]);
    expect(f).toEqual({ name: { $regex: 'al.*', $options: 'i' } });
  });

  it('in → $in (array passes through)', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'role', operator: 'in', value: ['a', 'b'] },
    ]);
    expect(f).toEqual({ role: { $in: ['a', 'b'] } });
  });

  it('in → $in (scalar wrapped)', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'role', operator: 'in', value: 'a' },
    ]);
    expect(f).toEqual({ role: { $in: ['a'] } });
  });

  it('in → $in (csv string split)', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'role', operator: 'in', value: 'a,b,c' },
    ]);
    expect(f).toEqual({ role: { $in: ['a', 'b', 'c'] } });
  });

  it('not in → $nin', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'role', operator: 'not in', value: ['a'] },
    ]);
    expect(f).toEqual({ role: { $nin: ['a'] } });
  });

  it('is null → null literal', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'deletedAt', operator: 'is null' },
    ]);
    expect(f).toEqual({ deletedAt: null });
  });

  it('is not null → $ne null', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'deletedAt', operator: 'is not null' },
    ]);
    expect(f).toEqual({ deletedAt: { $ne: null } });
  });

  it('_between → $gte + $lte object', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'age', operator: '_between', value: [10, 20] },
    ]);
    expect(f).toEqual({ age: { $gte: 10, $lte: 20 } });
  });

  it('_between → string CSV split', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'age', operator: '_between', value: '10,20' },
    ]);
    expect(f).toEqual({ age: { $gte: '10', $lte: '20' } });
  });

  it('_is_null=true → $eq null', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'deletedAt', operator: '_is_null', value: true },
    ]);
    expect(f).toEqual({ deletedAt: { $eq: null } });
  });

  it('_is_null=false → $ne null', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'deletedAt', operator: '_is_null', value: false },
    ]);
    expect(f).toEqual({ deletedAt: { $ne: null } });
  });

  it('_is_not_null=true → $ne null', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'deletedAt', operator: '_is_not_null', value: true },
    ]);
    expect(f).toEqual({ deletedAt: { $ne: null } });
  });

  it('_is_not_null=false → $eq null', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'deletedAt', operator: '_is_not_null', value: false },
    ]);
    expect(f).toEqual({ deletedAt: { $eq: null } });
  });

  it('id field → maps to _id in mongo mode', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'id', operator: '=', value: 'abc' },
    ]);
    expect(f).toEqual({ _id: 'abc' });
  });

  it('id field stays as id when dbType !== mongodb', () => {
    const f = whereToMongoFilter(
      emptyMetadata,
      [{ field: 'id', operator: '=', value: 'abc' }],
      'tbl',
      'postgres',
    );
    expect(f).toEqual({ id: 'abc' });
  });

  it('multiple conditions merge into AND-implicit object', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'name', operator: '=', value: 'alice' },
      { field: 'age', operator: '>', value: 18 },
    ]);
    expect(f).toEqual({ name: 'alice', age: { $gt: 18 } });
  });

  it('field with table prefix uses last segment', () => {
    const f = whereToMongoFilter(emptyMetadata, [
      { field: 'user.name', operator: '=', value: 'alice' },
    ]);
    expect(f).toEqual({ name: 'alice' });
  });

  it('empty conditions → empty filter', () => {
    expect(whereToMongoFilter(emptyMetadata, [])).toEqual({});
  });

  it('coverage check — handles every operator without throwing', () => {
    for (const op of ALL_OPERATORS) {
      const cond: WhereCondition = {
        field: 'f',
        operator: op,
        value:
          op === 'in' || op === 'not in'
            ? ['a']
            : op === '_between'
              ? [1, 2]
              : 'x',
      };
      expect(() => whereToMongoFilter(emptyMetadata, [cond])).not.toThrow();
    }
  });
});

describe('applyWhereToKnex — every operator covered', () => {
  function run(conditions: WhereCondition[]) {
    const { stub, calls } = makeKnexStub();
    applyWhereToKnex(stub, conditions, 'tbl', emptyMetadata, 'postgres');
    return calls;
  }

  it('= → query.where(field, =, value)', () => {
    expect(run([{ field: 'name', operator: '=', value: 'a' }])).toEqual([
      { method: 'where', args: ['name', '=', 'a'] },
    ]);
  });

  it('!= → query.where(field, !=, value)', () => {
    expect(run([{ field: 'name', operator: '!=', value: 'a' }])).toEqual([
      { method: 'where', args: ['name', '!=', 'a'] },
    ]);
  });

  it('> < >= <= → query.where with operator', () => {
    expect(run([{ field: 'age', operator: '>', value: 1 }])[0]).toEqual({
      method: 'where',
      args: ['age', '>', 1],
    });
    expect(run([{ field: 'age', operator: '<', value: 1 }])[0]).toEqual({
      method: 'where',
      args: ['age', '<', 1],
    });
    expect(run([{ field: 'age', operator: '>=', value: 1 }])[0]).toEqual({
      method: 'where',
      args: ['age', '>=', 1],
    });
    expect(run([{ field: 'age', operator: '<=', value: 1 }])[0]).toEqual({
      method: 'where',
      args: ['age', '<=', 1],
    });
  });

  it('like → query.where(field, like, value)', () => {
    expect(run([{ field: 'n', operator: 'like', value: '%x%' }])[0]).toEqual({
      method: 'where',
      args: ['n', 'like', '%x%'],
    });
  });

  it('in → whereIn (array passthrough)', () => {
    expect(run([{ field: 'r', operator: 'in', value: ['a', 'b'] }])[0]).toEqual(
      {
        method: 'whereIn',
        args: ['r', ['a', 'b']],
      },
    );
  });

  it('in → whereIn (scalar wrapped to array)', () => {
    expect(run([{ field: 'r', operator: 'in', value: 'a' }])[0]).toEqual({
      method: 'whereIn',
      args: ['r', ['a']],
    });
  });

  it('not in → whereNotIn', () => {
    expect(run([{ field: 'r', operator: 'not in', value: ['a'] }])[0]).toEqual({
      method: 'whereNotIn',
      args: ['r', ['a']],
    });
  });

  it('is null → whereNull', () => {
    expect(run([{ field: 'd', operator: 'is null' }])[0]).toEqual({
      method: 'whereNull',
      args: ['d'],
    });
  });

  it('is not null → whereNotNull', () => {
    expect(run([{ field: 'd', operator: 'is not null' }])[0]).toEqual({
      method: 'whereNotNull',
      args: ['d'],
    });
  });

  it('_contains → like %v%', () => {
    expect(
      run([{ field: 'n', operator: '_contains', value: 'foo' }])[0],
    ).toEqual({
      method: 'where',
      args: ['n', 'like', '%foo%'],
    });
  });

  it('_starts_with → like v%', () => {
    expect(
      run([{ field: 'n', operator: '_starts_with', value: 'foo' }])[0],
    ).toEqual({
      method: 'where',
      args: ['n', 'like', 'foo%'],
    });
  });

  it('_ends_with → like %v', () => {
    expect(
      run([{ field: 'n', operator: '_ends_with', value: 'foo' }])[0],
    ).toEqual({
      method: 'where',
      args: ['n', 'like', '%foo'],
    });
  });

  it('_between → whereBetween [a, b]', () => {
    expect(
      run([{ field: 'age', operator: '_between', value: [10, 20] }])[0],
    ).toEqual({
      method: 'whereBetween',
      args: ['age', [10, 20]],
    });
  });

  it('_between → CSV string split', () => {
    expect(
      run([{ field: 'age', operator: '_between', value: '10,20' }])[0],
    ).toEqual({
      method: 'whereBetween',
      args: ['age', ['10', '20']],
    });
  });

  it('_is_null=true → whereNull', () => {
    expect(run([{ field: 'd', operator: '_is_null', value: true }])[0]).toEqual(
      {
        method: 'whereNull',
        args: ['d'],
      },
    );
  });

  it('_is_null=false → whereNotNull', () => {
    expect(
      run([{ field: 'd', operator: '_is_null', value: false }])[0],
    ).toEqual({
      method: 'whereNotNull',
      args: ['d'],
    });
  });

  it('_is_not_null=true → whereNotNull', () => {
    expect(
      run([{ field: 'd', operator: '_is_not_null', value: true }])[0],
    ).toEqual({
      method: 'whereNotNull',
      args: ['d'],
    });
  });

  it('_is_not_null=false → whereNull', () => {
    expect(
      run([{ field: 'd', operator: '_is_not_null', value: false }])[0],
    ).toEqual({
      method: 'whereNull',
      args: ['d'],
    });
  });

  it('multiple conditions chain in order', () => {
    const calls = run([
      { field: 'a', operator: '=', value: 1 },
      { field: 'b', operator: '>', value: 2 },
    ]);
    expect(calls).toHaveLength(2);
    expect(calls[0]).toEqual({ method: 'where', args: ['a', '=', 1] });
    expect(calls[1]).toEqual({ method: 'where', args: ['b', '>', 2] });
  });

  it('coverage check — handles every operator without throwing', () => {
    for (const op of ALL_OPERATORS) {
      const cond: WhereCondition = {
        field: 'f',
        operator: op,
        value:
          op === 'in' || op === 'not in'
            ? ['a']
            : op === '_between'
              ? [1, 2]
              : 'x',
      };
      expect(() => run([cond])).not.toThrow();
    }
  });
});
