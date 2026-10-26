import { describe, expect, it, vi } from 'vitest';
import {
  matchFieldPermissionCondition as match,
  prepareFieldPermissionCondition,
} from '../../src/shared/utils/field-permission-condition.util';

function prepared(condition: unknown): unknown {
  const snapshot = structuredClone(condition);
  prepareFieldPermissionCondition(snapshot);
  return snapshot;
}

describe('prepared field permission conditions', () => {
  it('prepares metadata once without caching caller identity or record decisions', () => {
    const condition = prepared({
      _and: [
        { owner: { _eq: '@USER.id' } },
        { tenantId: { _in: '@USER.allowedTenantIds' } },
        { deleted: { _neq: true } },
      ],
    });
    const record = { owner: 7, tenantId: 2, deleted: false };
    expect(match(condition, record, { id: 7, allowedTenantIds: [2] })).toBe(
      true,
    );
    expect(match(condition, record, { id: 8, allowedTenantIds: [2] })).toBe(
      false,
    );
    expect(match(condition, record, { id: 7, allowedTenantIds: [3] })).toBe(
      false,
    );
    expect(
      match(
        condition,
        { ...record, deleted: true },
        { id: 7, allowedTenantIds: [2] },
      ),
    ).toBe(false);
    expect(match(condition, record, { id: 7 })).toBe(false);
  });

  it('does not enumerate condition keys again in the prepared hot path', () => {
    const condition = prepared({
      _and: [
        { owner: { _eq: '@USER.id' } },
        { status: { _in: ['draft', 'published'] } },
      ],
    });
    const keys = vi.spyOn(Object, 'keys');
    let result = false;
    let calls = 0;
    try {
      for (let index = 0; index < 20; index++)
        result = match(condition, { owner: 7, status: 'published' }, { id: 7 });
      calls = keys.mock.calls.length;
    } finally {
      keys.mockRestore();
    }
    expect(result).toBe(true);
    expect(calls).toBe(0);
  });

  it('protects prepared snapshots while leaving mutable direct conditions live', () => {
    const source = { owner: { _eq: 7 } };
    const snapshot = prepared(source) as typeof source;
    expect(Object.isFrozen(snapshot)).toBe(true);
    expect(Object.isFrozen(snapshot.owner)).toBe(true);
    expect(() => {
      snapshot.owner._eq = 8;
    }).toThrow(TypeError);
    source.owner._eq = 8;
    expect(match(source, { owner: 7 }, {})).toBe(false);
    expect(match(source, { owner: 8 }, {})).toBe(true);
    expect(match(snapshot, { owner: 7 }, {})).toBe(true);
  });

  it('preserves deny behavior across operators and nested condition shapes', () => {
    const values = [
      null,
      false,
      true,
      0,
      1,
      7,
      8,
      -1,
      '',
      '7',
      'draft',
      'published',
      '2026-01-01T00:00:00Z',
      {},
      { id: 7 },
      { _id: '7' },
      [7, 8],
    ];
    const operators = [
      '_eq',
      '_neq',
      '_gt',
      '_gte',
      '_lt',
      '_lte',
      '_in',
      '_not_in',
      '_nin',
      '_is_null',
      '_is_not_null',
      '_unknown',
    ];
    const user = { id: 7, allowed: [7, 8] };
    let denied = 0;
    let allowed = 0;
    for (const operator of operators) {
      for (const expected of [
        ...values,
        '@USER',
        '@USER.id',
        '@USER.missing',
        '@USER.allowed',
      ]) {
        const condition = { value: { [operator]: expected } };
        const snapshot = prepared(condition);
        for (const actual of values) {
          const record = { value: actual };
          const result = match(condition, record, user);
          expect(match(snapshot, record, user)).toBe(result);
          if (result) allowed++;
          else denied++;
        }
      }
    }
    const conditions = [
      null,
      [],
      {},
      'invalid',
      { _and: [] },
      { _or: [] },
      { _and: [null] },
      { _or: [null] },
      { _not: null },
      { _unknown: 1 },
      { _and: [{ value: { _eq: 7 } }, { _not: { deleted: { _eq: true } } }] },
      {
        _or: [
          { owner: { id: { _eq: '@USER.id' } } },
          { value: { _in: [7, '@USER.id'] } },
        ],
      },
    ];
    for (const condition of conditions) {
      const snapshot = prepared(condition);
      for (const value of values) {
        const record = { value, owner: { _id: 7 }, deleted: false };
        expect(match(snapshot, record, user)).toBe(
          match(condition, record, user),
        );
      }
    }
    expect(allowed).toBeGreaterThan(0);
    expect(denied).toBeGreaterThan(allowed);
  });

  it('preserves sparse arrays and short-circuit evaluation', () => {
    const holes = new Array(3);
    for (const condition of [
      { _and: holes },
      { _or: holes },
      { owner: { _in: holes } },
      { owner: { _nin: holes } },
    ]) {
      expect(match(prepared(condition), { owner: 7 }, {})).toBe(
        match(condition, { owner: 7 }, {}),
      );
    }
    const read = vi.fn(() => {
      throw new Error('must not read');
    });
    const record = {
      owner: 7,
      get secret() {
        return read();
      },
    };
    const condition = prepared({
      _and: [{ owner: { _eq: 8 } }, { secret: { _eq: 1 } }],
    });
    expect(match(condition, record, {})).toBe(false);
    expect(read).not.toHaveBeenCalled();
  });

  it('preserves record access order when a later condition is invalid', () => {
    const read = vi.fn(() => 7);
    const record = {
      get owner() {
        return read();
      },
    };
    expect(
      match(prepared({ owner: { _eq: 7 }, _unknown: 1 }), record, {}),
    ).toBe(false);
    expect(read).toHaveBeenCalledTimes(1);
    read.mockClear();
    expect(
      match(prepared({ owner: { _eq: 7, _unknown: 1 } }), record, {}),
    ).toBe(false);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it('keeps mutable non-JSON values and accessors on the interpreter path', () => {
    const date = new Date('2026-01-01T00:00:00Z');
    const condition = { createdAt: { _gte: date } };
    prepareFieldPermissionCondition(condition);
    expect(Object.isFrozen(condition)).toBe(false);
    expect(match(condition, { createdAt: new Date('2026-06-01') }, {})).toBe(
      true,
    );
    date.setFullYear(2027);
    expect(match(condition, { createdAt: new Date('2026-06-01') }, {})).toBe(
      false,
    );
    const getter = vi.fn(() => ({ _eq: 7 }));
    const accessor = {
      get owner() {
        return getter();
      },
    };
    prepareFieldPermissionCondition(accessor);
    expect(getter).not.toHaveBeenCalled();
    expect(match(accessor, { owner: 7 }, {})).toBe(true);
    expect(getter).toHaveBeenCalledTimes(1);
    const ownKeys = vi.fn(() => ['owner']);
    const proxy = new Proxy({ owner: { _eq: 7 } }, { ownKeys });
    prepareFieldPermissionCondition(proxy);
    expect(ownKeys).not.toHaveBeenCalled();
    expect(Object.isFrozen(proxy)).toBe(false);
    expect(match(proxy, { owner: 7 }, {})).toBe(true);
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => prepareFieldPermissionCondition(cyclic)).not.toThrow();
    expect(Object.isFrozen(cyclic)).toBe(false);
  });
});
