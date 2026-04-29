import { describe, it, expect, beforeEach } from 'vitest';
import { applyOperatorToMatch } from '../../src/kernel/query';

const SUPPORTED_OPS = [
  '_contains',
  '_starts_with',
  '_ends_with',
  '_eq',
  '_neq',
  '_in',
  '_not_in',
  '_nin',
  '_gt',
  '_gte',
  '_lt',
  '_lte',
  '_is_null',
  '_is_not_null',
  '_between',
] as const;

const emptyMetadata = { tables: new Map() };
const nullableMetadata = {
  tables: new Map([['t', { columns: [{ name: 'f', isNullable: true }] }]]),
};
const nonNullableMetadata = {
  tables: new Map([['t', { columns: [{ name: 'f', isNullable: false }] }]]),
};

describe('applyOperatorToMatch — supported operators', () => {
  let match: any;
  beforeEach(() => {
    match = {};
  });

  it('_eq sets field directly', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_eq', 'x');
    expect(match).toEqual({ f: 'x' });
  });

  it('_neq nullable column → $and with $ne null + $ne value', () => {
    applyOperatorToMatch(nullableMetadata, match, 't', 'f', '_neq', 'x');
    expect(match).toEqual({
      $and: [{ f: { $ne: null } }, { f: { $ne: 'x' } }],
    });
  });

  it('_neq non-nullable column → simple $ne', () => {
    applyOperatorToMatch(nonNullableMetadata, match, 't', 'f', '_neq', 'x');
    expect(match).toEqual({ f: { $ne: 'x' } });
  });

  it('_in array passes through', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_in', ['a', 'b']);
    expect(match).toEqual({ f: { $in: ['a', 'b'] } });
  });

  it('_in scalar wrapped to array', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_in', 'a');
    expect(match).toEqual({ f: { $in: ['a'] } });
  });

  it('_in CSV string split', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_in', 'a,b');
    expect(match).toEqual({ f: { $in: ['a', 'b'] } });
  });

  it('_not_in nullable column → $and clause', () => {
    applyOperatorToMatch(nullableMetadata, match, 't', 'f', '_not_in', ['x']);
    expect(match).toEqual({
      $and: [{ f: { $ne: null } }, { f: { $nin: ['x'] } }],
    });
  });

  it('_nin alias works same as _not_in', () => {
    applyOperatorToMatch(nonNullableMetadata, match, 't', 'f', '_nin', ['x']);
    expect(match).toEqual({ f: { $nin: ['x'] } });
  });

  it('_gt → $gt', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_gt', 1);
    expect(match).toEqual({ f: { $gt: 1 } });
  });

  it('_gte → $gte', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_gte', 1);
    expect(match).toEqual({ f: { $gte: 1 } });
  });

  it('_lt → $lt', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_lt', 1);
    expect(match).toEqual({ f: { $lt: 1 } });
  });

  it('_lte → $lte', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_lte', 1);
    expect(match).toEqual({ f: { $lte: 1 } });
  });

  it('_is_null=true → $eq null', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_is_null', true);
    expect(match).toEqual({ f: { $eq: null } });
  });

  it('_is_null=false → $ne null', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_is_null', false);
    expect(match).toEqual({ f: { $ne: null } });
  });

  it("_is_null='true' (string) → $eq null", () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_is_null', 'true');
    expect(match).toEqual({ f: { $eq: null } });
  });

  it('_is_not_null=true → $ne null', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_is_not_null', true);
    expect(match).toEqual({ f: { $ne: null } });
  });

  it('_is_not_null=false → $eq null', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_is_not_null', false);
    expect(match).toEqual({ f: { $eq: null } });
  });

  it('_between [a, b] → $gte+$lte', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_between', [1, 10]);
    expect(match).toEqual({ f: { $gte: 1, $lte: 10 } });
  });

  it('_between CSV string → split into array', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_between', '1,10');
    expect(match).toEqual({ f: { $gte: '1', $lte: '10' } });
  });

  it('_between with non-pair value → no-op (silent)', () => {
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_between', [1]);
    expect(match).toEqual({});
  });
});

describe('applyOperatorToMatch — unknown operator is a silent no-op (intentional defensive boundary)', () => {
  it('unknown operator → match object unchanged', () => {
    const match: any = {};
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_bogus_op', 'x');
    expect(match).toEqual({});
  });

  it('unknown operator does not throw (filter-AST already validates upstream)', () => {
    const match: any = {};
    expect(() =>
      applyOperatorToMatch(emptyMetadata, match, 't', 'f', '_bogus_op', 'x'),
    ).not.toThrow();
  });

  it('unknown operator preserves existing match state', () => {
    const match: any = { existing: 'value' };
    applyOperatorToMatch(emptyMetadata, match, 't', 'f', 'unknown', 'x');
    expect(match).toEqual({ existing: 'value' });
  });
});

describe('applyOperatorToMatch — coverage of supported list', () => {
  it('every operator in SUPPORTED_OPS does not throw', () => {
    for (const op of SUPPORTED_OPS) {
      const match: any = {};
      const val =
        op === '_in' || op === '_not_in' || op === '_nin'
          ? ['a']
          : op === '_between'
            ? [1, 2]
            : 'x';
      expect(() =>
        applyOperatorToMatch(emptyMetadata, match, 't', 'f', op, val),
      ).not.toThrow();
    }
  });
});
