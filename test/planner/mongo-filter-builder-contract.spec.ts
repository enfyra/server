import { describe, expect, it } from 'vitest';
import { applyOperatorToMatch, whereToMongoFilter } from '@enfyra/kernel';

const nullableMetadata = {
  tables: new Map([
    [
      'users',
      {
        columns: [
          { name: 'age', isNullable: false },
          { name: 'name', isNullable: true },
          { name: 'role', isNullable: true },
        ],
      },
    ],
  ]),
};

describe('Mongo filter builder contracts', () => {
  it('preserves Mongo dotted paths and uses only explicit tables for conversion', () => {
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'address.city', operator: '=', value: 'Paris' }],
        'users',
        'mongodb',
      ),
    ).toEqual({ 'address.city': 'Paris' });
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'users.age', operator: '=', value: 18 }],
        'users',
        'mongodb',
      ),
    ).toEqual({ age: 18 });

    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'age', operator: '=', value: 18 }],
        undefined,
        'mongodb',
      ),
    ).toEqual({ age: 18 });
  });

  it('preserves repeated predicates for the same field', () => {
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [
          { field: 'age', operator: '>=', value: 18 },
          { field: 'age', operator: '<=', value: 65 },
        ],
        'users',
        'mongodb',
      ),
    ).toEqual({
      $and: [{ age: { $gte: 18 } }, { age: { $lte: 65 } }],
    });

    const match: Record<string, unknown> = {};
    applyOperatorToMatch(
      nullableMetadata,
      match,
      'users',
      'age',
      '_gte',
      18,
    );
    applyOperatorToMatch(
      nullableMetadata,
      match,
      'users',
      'age',
      '_lte',
      65,
    );
    expect(match).toEqual({
      $and: [{ age: { $gte: 18 } }, { age: { $lte: 65 } }],
    });
  });

  it('uses literal equality for operator-shaped object values', () => {
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'name', operator: '=', value: { $ne: null } }],
        'users',
        'mongodb',
      ),
    ).toEqual({ name: { $eq: { $ne: null } } });

    const match: Record<string, unknown> = {};
    applyOperatorToMatch(
      nullableMetadata,
      match,
      'users',
      'name',
      '_eq',
      { $ne: null },
    );
    expect(match).toEqual({ name: { $eq: { $ne: null } } });
  });

  it('translates SQL LIKE wildcards without executing regex syntax', () => {
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'name', operator: 'like', value: 'a.c_%' }],
        'users',
        'mongodb',
      ),
    ).toEqual({
      name: { $regex: '^a\\.c[\\s\\S][\\s\\S]*(?![\\s\\S])', $options: 'i' },
    });
  });

  it('preserves SQL null semantics for IN and NOT IN lists', () => {
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'role', operator: 'in', value: ['admin', null] }],
        'users',
        'mongodb',
      ),
    ).toEqual({ role: { $in: ['admin'] } });
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'role', operator: 'not in', value: ['admin', null] }],
        'users',
        'mongodb',
      ),
    ).toEqual({ $and: [{ _id: { $exists: false, $type: 'null' } }] });
  });

  it('excludes null and missing values for SQL-style negative predicates', () => {
    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'name', operator: '!=', value: 'alice' }],
        'users',
        'mongodb',
      ),
    ).toEqual({
      $and: [{ name: { $ne: null } }, { name: { $ne: 'alice' } }],
    });

    expect(
      whereToMongoFilter(
        nullableMetadata,
        [{ field: 'role', operator: 'not in', value: ['admin'] }],
        'users',
        'mongodb',
      ),
    ).toEqual({
      $and: [{ role: { $ne: null } }, { role: { $nin: ['admin'] } }],
    });
  });

  it('rejects Mongo operator and prototype path segments', () => {
    for (const field of ['$where', '$expr', '__proto__', 'constructor.value']) {
      expect(() =>
        whereToMongoFilter(
          nullableMetadata,
          [{ field, operator: '=', value: true }],
          'users',
          'mongodb',
        ),
      ).toThrow('Invalid MongoDB field');

      expect(() =>
        applyOperatorToMatch(
          nullableMetadata,
          {},
          'users',
          field,
          '_eq',
          true,
        ),
      ).toThrow('Invalid MongoDB field');
    }
  });

  it('keeps folded-text state out of the document field namespace', () => {
    const direct = whereToMongoFilter(
      nullableMetadata,
      [
        {
          field: '__mongoFoldTextTriples',
          operator: '=',
          value: 'stored value',
        },
      ],
      'users',
      'mongodb',
    );
    expect(direct).toEqual({ __mongoFoldTextTriples: 'stored value' });

    const match: Record<string, unknown> = {};
    applyOperatorToMatch(
      nullableMetadata,
      match,
      'users',
      'name',
      '_contains',
      'alice',
    );
    expect(match).not.toHaveProperty('__mongoFoldTextTriples');
    expect(match).toHaveProperty('$expr');
  });
});
