import { describe, expect, it } from 'vitest';
import { hasLogicalOperators } from '@enfyra/kernel';

describe('logical operator detection contracts', () => {
  it('handles circular objects without recursion failure', () => {
    const filter: any = { field: { _eq: 1 } };
    filter.self = filter;
    expect(hasLogicalOperators(filter)).toBe(false);
  });

  it('does not classify logical-looking keys inside literal operator values', () => {
    expect(
      hasLogicalOperators({ payload: { _eq: { _or: 'stored data' } } }),
    ).toBe(false);
  });

  it('does not iterate sparse array holes', () => {
    const sparse: unknown[] = [];
    sparse.length = 0xffffffff;
    expect(hasLogicalOperators({ _eq: sparse })).toBe(false);
  });

  it('fails closed without invoking throwing getters', () => {
    const filter = Object.defineProperty({}, 'value', {
      enumerable: true,
      get() {
        throw new Error('getter invoked');
      },
    });
    expect(hasLogicalOperators(filter)).toBe(true);
  });

  it('detects logical operators beyond deep ordinary nesting', () => {
    let filter: any = { _not: { field: { _eq: 1 } } };
    for (let index = 0; index < 10_000; index += 1) {
      filter = { nested: filter };
    }
    expect(hasLogicalOperators(filter)).toBe(true);
  });
});
