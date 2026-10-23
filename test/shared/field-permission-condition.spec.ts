import { describe, it, expect, vi } from 'vitest';
import {
  getFieldPermissionConditionFields,
  matchFieldPermissionCondition as match,
  prepareFieldPermissionCondition,
} from '../../src/shared/utils/field-permission-condition.util';
import {
  buildRequestedShapeFromQuery,
  sanitizeFieldPermissionsResult,
} from '../../src/shared/utils/sanitize-field-permissions.util';

describe('matchFieldPermissionCondition', () => {
  describe('shape / safety', () => {
    it('returns false for non-object condition', () => {
      expect(match(null, {}, {})).toBe(false);
      expect(match(undefined, {}, {})).toBe(false);
      expect(match('x', {}, {})).toBe(false);
      expect(match([{ a: 1 }], {}, {})).toBe(false);
    });

    it('returns true for empty condition (degenerate accept)', () => {
      expect(match({}, { id: 1 }, { id: 1 })).toBe(true);
    });

    it('rejects unknown operator key at root (underscore prefix)', () => {
      expect(match({ _bogus: 1 }, { id: 1 }, {})).toBe(false);
    });

    it('rejects unknown operator inside field node', () => {
      expect(match({ id: { _foo: 1 } }, { id: 1 }, {})).toBe(false);
    });
  });

  describe('_eq / _neq', () => {
    it('matches id equality via macro (@USER.id)', () => {
      expect(match({ id: { _eq: '@USER.id' } }, { id: 5 }, { id: 5 })).toBe(
        true,
      );
      expect(match({ id: { _eq: '@USER.id' } }, { id: '5' }, { id: 5 })).toBe(
        true,
      );
      expect(match({ id: { _eq: '@USER.id' } }, { id: 5 }, { id: 6 })).toBe(
        false,
      );
    });

    it('matches id via _id fallback (mongo record)', () => {
      expect(
        match({ id: { _eq: '@USER.id' } }, { _id: 'abc' }, { _id: 'abc' }),
      ).toBe(true);
    });

    it('_neq flips', () => {
      expect(match({ id: { _neq: '@USER.id' } }, { id: 5 }, { id: 6 })).toBe(
        true,
      );
      expect(match({ id: { _neq: '@USER.id' } }, { id: 5 }, { id: 5 })).toBe(
        false,
      );
    });

    it('macro resolving undefined → fail-closed', () => {
      expect(
        match({ id: { _eq: '@USER.missing' } }, { id: 5 }, { id: 5 }),
      ).toBe(false);
    });
  });

  describe('ordered comparisons', () => {
    it('_gt / _gte / _lt / _lte on numbers', () => {
      expect(match({ count: { _gt: 5 } }, { count: 10 }, {})).toBe(true);
      expect(match({ count: { _gt: 10 } }, { count: 10 }, {})).toBe(false);
      expect(match({ count: { _gte: 10 } }, { count: 10 }, {})).toBe(true);
      expect(match({ count: { _lt: 10 } }, { count: 5 }, {})).toBe(true);
      expect(match({ count: { _lte: 5 } }, { count: 5 }, {})).toBe(true);
    });

    it('numeric string coerces to number', () => {
      expect(match({ count: { _gt: '5' } }, { count: '10' }, {})).toBe(true);
    });

    it('compares ISO date strings', () => {
      expect(
        match(
          { createdAt: { _gt: '2020-01-01T00:00:00Z' } },
          { createdAt: '2021-06-15T00:00:00Z' },
          {},
        ),
      ).toBe(true);
    });

    it('null actual → fail-closed', () => {
      expect(match({ count: { _gt: 5 } }, { count: null }, {})).toBe(false);
      expect(match({ count: { _gt: 5 } }, {}, {})).toBe(false);
    });
  });

  describe('_in / _not_in / _nin', () => {
    it('_in with literal array', () => {
      expect(
        match(
          { status: { _in: ['draft', 'review'] } },
          { status: 'draft' },
          {},
        ),
      ).toBe(true);
      expect(
        match(
          { status: { _in: ['draft', 'review'] } },
          { status: 'published' },
          {},
        ),
      ).toBe(false);
    });

    it('_in resolves macros inside array', () => {
      expect(
        match(
          { tenantId: { _in: ['@USER.tenantId', 999] } },
          { tenantId: 42 },
          { tenantId: 42 },
        ),
      ).toBe(true);
    });

    it('_in with macro resolving to array', () => {
      expect(
        match(
          { tagId: { _in: '@USER.allowedTagIds' } },
          { tagId: 2 },
          { allowedTagIds: [1, 2, 3] },
        ),
      ).toBe(true);
      expect(
        match(
          { tagId: { _in: '@USER.allowedTagIds' } },
          { tagId: 9 },
          { allowedTagIds: [1, 2, 3] },
        ),
      ).toBe(false);
    });

    it('_in with non-array macro → fail-closed', () => {
      expect(
        match(
          { tagId: { _in: '@USER.something' } },
          { tagId: 2 },
          { something: 'scalar' },
        ),
      ).toBe(false);
    });

    it('_not_in and _nin are symmetric', () => {
      expect(
        match({ status: { _not_in: ['published'] } }, { status: 'draft' }, {}),
      ).toBe(true);
      expect(
        match({ status: { _nin: ['draft'] } }, { status: 'draft' }, {}),
      ).toBe(false);
    });
  });

  describe('_is_null / _is_not_null', () => {
    it('_is_null true/false', () => {
      expect(match({ deletedAt: { _is_null: true } }, {}, {})).toBe(true);
      expect(
        match({ deletedAt: { _is_null: true } }, { deletedAt: null }, {}),
      ).toBe(true);
      expect(
        match({ deletedAt: { _is_null: true } }, { deletedAt: 'x' }, {}),
      ).toBe(false);
      expect(
        match({ deletedAt: { _is_null: false } }, { deletedAt: 'x' }, {}),
      ).toBe(true);
    });

    it('_is_not_null', () => {
      expect(
        match({ email: { _is_not_null: true } }, { email: 'a@b' }, {}),
      ).toBe(true);
      expect(match({ email: { _is_not_null: true } }, {}, {})).toBe(false);
    });
  });

  describe('logical _and / _or / _not', () => {
    it('_and requires every child', () => {
      expect(
        match(
          {
            _and: [
              { status: { _eq: 'draft' } },
              { ownerId: { _eq: '@USER.id' } },
            ],
          },
          { status: 'draft', ownerId: 5 },
          { id: 5 },
        ),
      ).toBe(true);
      expect(
        match(
          {
            _and: [
              { status: { _eq: 'draft' } },
              { ownerId: { _eq: '@USER.id' } },
            ],
          },
          { status: 'draft', ownerId: 6 },
          { id: 5 },
        ),
      ).toBe(false);
    });

    it('_or requires any', () => {
      expect(
        match(
          { _or: [{ status: { _eq: 'draft' } }, { ownerId: { _eq: 9 } }] },
          { status: 'published', ownerId: 9 },
          {},
        ),
      ).toBe(true);
    });

    it('empty _and/_or → fail-closed', () => {
      expect(match({ _and: [] }, {}, {})).toBe(false);
      expect(match({ _or: [] }, {}, {})).toBe(false);
    });

    it('_not inverts', () => {
      expect(
        match({ _not: { status: { _eq: 'draft' } } }, { status: 'draft' }, {}),
      ).toBe(false);
      expect(
        match(
          { _not: { status: { _eq: 'draft' } } },
          { status: 'published' },
          {},
        ),
      ).toBe(true);
    });

    it('_not with non-object → fail-closed', () => {
      expect(match({ _not: null }, {}, {})).toBe(false);
    });
  });

  describe('nested many-to-one path traversal', () => {
    it('traverses one level (record.owner.id)', () => {
      expect(
        match(
          { owner: { id: { _eq: '@USER.id' } } },
          { owner: { id: 5 } },
          { id: 5 },
        ),
      ).toBe(true);
    });

    it('traverses two levels (record.tenant.plan.name)', () => {
      expect(
        match(
          { tenant: { plan: { name: { _eq: 'pro' } } } },
          { tenant: { plan: { name: 'pro' } } },
          {},
        ),
      ).toBe(true);
    });

    it('relation not loaded → fail-closed', () => {
      expect(match({ owner: { id: { _eq: '@USER.id' } } }, {}, { id: 5 })).toBe(
        false,
      );
      expect(
        match(
          { owner: { id: { _eq: '@USER.id' } } },
          { owner: null },
          { id: 5 },
        ),
      ).toBe(false);
    });

    it('array relation (o2m/m2m) not allowed → fail-closed', () => {
      expect(
        match(
          { tags: { id: { _eq: 1 } } },
          { tags: [{ id: 1 }, { id: 2 }] },
          {},
        ),
      ).toBe(false);
    });

    it('m2o stored as raw FK value (not object) → fail-closed', () => {
      expect(
        match({ owner: { id: { _eq: '@USER.id' } } }, { owner: 5 }, { id: 5 }),
      ).toBe(false);
    });
  });

  describe('@USER macro paths', () => {
    it('@USER and @USER.id / @USER._id', () => {
      expect(match({ id: { _eq: '@USER' } }, { id: 5 }, { id: 5 })).toBe(true);
      expect(
        match({ id: { _eq: '@USER._id' } }, { id: 'a' }, { _id: 'a' }),
      ).toBe(true);
    });

    it('@USER.role.id', () => {
      expect(
        match(
          { roleId: { _eq: '@USER.role.id' } },
          { roleId: 7 },
          { id: 1, role: { id: 7, name: 'admin' } },
        ),
      ).toBe(true);
    });

    it('@USER.role.name', () => {
      expect(
        match(
          { tenantRoleName: { _eq: '@USER.role.name' } },
          { tenantRoleName: 'admin' },
          { role: { name: 'admin' } },
        ),
      ).toBe(true);
    });

    it('@USER path not resolvable → fail-closed', () => {
      expect(
        match({ roleId: { _eq: '@USER.role.id' } }, { roleId: 7 }, { id: 1 }),
      ).toBe(false);
    });

    it('@USER macro only in value, not in field key', () => {
      expect(
        match({ '@USER.id': { _eq: 1 } }, { '@USER.id': 1 }, {}),
      ).toBe(false);
    });
  });

  describe('real-world scenarios', () => {
    it('self-update password: id == @USER.id', () => {
      const cond = { id: { _eq: '@USER.id' } };
      expect(match(cond, { id: 42 }, { id: 42 })).toBe(true);
      expect(match(cond, { id: 99 }, { id: 42 })).toBe(false);
    });

    it('self-update via mongo _id', () => {
      const cond = { id: { _eq: '@USER.id' } };
      expect(match(cond, { _id: 'u1' }, { _id: 'u1' })).toBe(true);
    });

    it('owner + non-published composite', () => {
      const cond = {
        _and: [
          { ownerId: { _eq: '@USER.id' } },
          { status: { _neq: 'published' } },
        ],
      };
      expect(match(cond, { ownerId: 5, status: 'draft' }, { id: 5 })).toBe(
        true,
      );
      expect(match(cond, { ownerId: 5, status: 'published' }, { id: 5 })).toBe(
        false,
      );
      expect(match(cond, { ownerId: 6, status: 'draft' }, { id: 5 })).toBe(
        false,
      );
    });

    it('tenant whitelist via @USER.allowedTenantIds', () => {
      const cond = { tenantId: { _in: '@USER.allowedTenantIds' } };
      expect(
        match(cond, { tenantId: 2 }, { allowedTenantIds: [1, 2, 3] }),
      ).toBe(true);
      expect(
        match(cond, { tenantId: 9 }, { allowedTenantIds: [1, 2, 3] }),
      ).toBe(false);
    });

    it('soft-delete gate', () => {
      const cond = { deletedAt: { _is_null: true } };
      expect(match(cond, {}, {})).toBe(true);
      expect(match(cond, { deletedAt: '2024-01-01' }, {})).toBe(false);
    });
  });
});

describe('field permission unknown boundaries', () => {
  const modes = [false, true];

  it.each(modes)('preserves primitive and date ordered coercion with prepared=%s', (prepared) => {
    const cases: Array<{ actual: unknown; expected: unknown; operator: string; allowed: boolean }> = [
      { actual: true, expected: '0', operator: '_gt', allowed: true },
      { actual: false, expected: 0, operator: '_gte', allowed: true },
      { actual: '10', expected: 2, operator: '_gt', allowed: true },
      { actual: 'beta', expected: 'alpha', operator: '_gt', allowed: true },
      { actual: ' ', expected: '', operator: '_gt', allowed: true },
      { actual: Infinity, expected: 100, operator: '_gt', allowed: true },
      { actual: NaN, expected: NaN, operator: '_gte', allowed: true },
      { actual: NaN, expected: 0, operator: '_gt', allowed: false },
      { actual: 'Infinity', expected: Infinity, operator: '_gte', allowed: false },
      { actual: 'alpha', expected: 0, operator: '_gt', allowed: false },
      { actual: null, expected: 0, operator: '_gte', allowed: false },
      { actual: new Date('2026-06-01'), expected: '2026-01-01', operator: '_gt', allowed: true },
      { actual: new Date('invalid'), expected: 0, operator: '_gte', allowed: false },
    ];
    for (const { actual, expected, operator, allowed } of cases) {
      const condition = { value: { [operator]: expected } };
      if (prepared) prepareFieldPermissionCondition(condition);
      expect(match(condition, { value: actual }, {})).toBe(allowed);
    }
  });

  it.each(modes)('preserves identity, scalar and null equality with prepared=%s', (prepared) => {
    const cases: Array<{ actual: unknown; expected: unknown; allowed: boolean }> = [
      { actual: true, expected: 'true', allowed: true },
      { actual: true, expected: 1, allowed: false },
      { actual: 42, expected: '42', allowed: true },
      { actual: NaN, expected: 'NaN', allowed: true },
      { actual: undefined, expected: null, allowed: true },
      { actual: null, expected: undefined, allowed: false },
      { actual: { _id: 7, id: 8 }, expected: 7, allowed: true },
      { actual: { _id: null, id: 8 }, expected: '8', allowed: true },
      { actual: {}, expected: '', allowed: true },
      { actual: [], expected: '', allowed: true },
      { actual: new Date('2026-01-01'), expected: '', allowed: true },
      { actual: 42n, expected: '42', allowed: true },
      { actual: Symbol.for('field-policy'), expected: 'Symbol(field-policy)', allowed: true },
    ];
    for (const { actual, expected, allowed } of cases) {
      const condition = { value: { _eq: expected } };
      if (prepared) prepareFieldPermissionCondition(condition);
      expect(match(condition, { value: actual }, {})).toBe(allowed);
    }
  });

  it.each(modes)('retains object identity conversion rather than object toString with prepared=%s', (prepared) => {
    const stringify = vi.fn(() => 'unexpected');
    const selectedId = { toString: vi.fn(() => 'selected-id') };
    const condition = { value: { _eq: '' }, selected: { _eq: 'selected-id' } };
    if (prepared) prepareFieldPermissionCondition(condition);

    expect(match(condition, {
      value: { toString: stringify },
      selected: { _id: selectedId },
    }, {})).toBe(true);
    expect(stringify).not.toHaveBeenCalled();
    expect(selectedId.toString).toHaveBeenCalledTimes(1);
  });

  it.each(modes)('preserves property lookup order without adding proxy has checks with prepared=%s', (prepared) => {
    const reads: string[] = [];
    const identity = new Proxy({ _id: 'identity' }, {
      get(target, key, receiver) {
        reads.push(`identity:${String(key)}`);
        return Reflect.get(target, key, receiver);
      },
      has() { throw new Error('must not probe identity'); },
    });
    const record = new Proxy({ _id: identity }, {
      get(target, key, receiver) {
        reads.push(`record:${String(key)}`);
        return Reflect.get(target, key, receiver);
      },
      has() { throw new Error('must not probe record'); },
    });
    const condition = { id: { _eq: 'identity' } };
    if (prepared) prepareFieldPermissionCondition(condition);

    expect(match(condition, record, {})).toBe(true);
    expect(reads).toEqual(['record:id', 'record:_id', 'identity:_id']);
  });

  it.each(modes)('retains inherited accessors and their receiver with prepared=%s', (prepared) => {
    class RecordWithOwner {
      ownerId = 7;
      get owner() { return { id: this.ownerId }; }
    }
    const record = new RecordWithOwner();
    const condition = { 'owner.id': { _eq: '@USER.id' } };
    if (prepared) prepareFieldPermissionCondition(condition);

    expect(match(condition, record, { _id: 7 })).toBe(true);
    record.ownerId = 8;
    expect(match(condition, record, { _id: 7 })).toBe(false);
  });

  it.each(modes)('retains missing macro and membership behavior with prepared=%s', (prepared) => {
    for (const operator of ['_eq', '_neq', '_gt', '_gte', '_lt', '_lte', '_in', '_nin']) {
      const condition = { value: { [operator]: '@USER.missing' } };
      if (prepared) prepareFieldPermissionCondition(condition);
      expect(match(condition, { value: 7 }, {})).toBe(false);
    }
    const membership = { value: { _in: ['@USER.missing'] } };
    if (prepared) prepareFieldPermissionCondition(membership);
    expect(match(membership, {}, {})).toBe(true);
    expect(match(membership, { value: 7 }, {})).toBe(false);
    const prefix = { value: { _eq: '@USERX' } };
    if (prepared) prepareFieldPermissionCondition(prefix);
    expect(match(prefix, { value: 7 }, { '': 7 })).toBe(true);
  });

  it.each(modes)('preserves lexical object identities without numeric re-coercion with prepared=%s', (prepared) => {
    const lexical = { value: { _gt: { id: 10 } } };
    const mixed = { value: { _gt: 10 } };
    if (prepared) {
      prepareFieldPermissionCondition(lexical);
      prepareFieldPermissionCondition(mixed);
    }
    expect(match(lexical, { value: { _id: 2 } }, {})).toBe(true);
    expect(match(mixed, { value: { _id: 20 } }, {})).toBe(false);
  });

  it.each(modes)('retains sparse arrays and logical short-circuit with prepared=%s', (prepared) => {
    const holes: unknown[] = new Array(2);
    const cases = [
      { condition: { _and: holes }, allowed: true },
      { condition: { _or: holes }, allowed: false },
      { condition: { value: { _in: holes } }, allowed: false },
      { condition: { value: { _nin: holes } }, allowed: true },
    ];
    for (const { condition, allowed } of cases) {
      if (prepared) prepareFieldPermissionCondition(condition);
      expect(match(condition, { value: 7 }, {})).toBe(allowed);
    }
    const read = vi.fn(() => { throw new Error('must short-circuit'); });
    const record = { value: 7, get secret() { return read(); } };
    const condition = { _and: [{ value: { _eq: 8 } }, { secret: { _eq: 7 } }] };
    if (prepared) prepareFieldPermissionCondition(condition);
    expect(match(condition, record, {})).toBe(false);
    expect(read).not.toHaveBeenCalled();
  });

  it.each(modes)('does not swallow identity or date conversion errors with prepared=%s', (prepared) => {
    const error = new Error('identity getter failed');
    const record = { value: { get _id() { throw error; } } };
    const condition = { value: { _eq: 'identity' } };
    if (prepared) prepareFieldPermissionCondition(condition);
    expect(() => match(condition, record, {})).toThrow(error);

    const date = new Date();
    date.getTime = () => { throw error; };
    const ordered = { value: { _gte: 0 } };
    if (prepared) prepareFieldPermissionCondition(ordered);
    expect(() => match(ordered, { value: date }, {})).toThrow(error);
  });

  it('does not inspect nested proxies or invoke descriptors during preparation', () => {
    const traps = vi.fn(() => { throw new Error('must not inspect proxy'); });
    const proxy = new Proxy({ _eq: 7 }, {
      getPrototypeOf: traps,
      ownKeys: traps,
      getOwnPropertyDescriptor: traps,
    });
    const condition = { owner: proxy };
    prepareFieldPermissionCondition(condition);
    expect(traps).not.toHaveBeenCalled();
    expect(Object.isFrozen(condition)).toBe(false);

    const read = vi.fn(() => 7);
    const accessor = { owner: { get _eq() { return read(); } } };
    prepareFieldPermissionCondition(accessor);
    expect(read).not.toHaveBeenCalled();
    expect(Object.isFrozen(accessor)).toBe(false);
    expect(match(accessor, { owner: 7 }, {})).toBe(true);
    expect(read).toHaveBeenCalledTimes(1);
  });

  it('does not freeze cyclic, symbolic or custom-prototype condition graphs', () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const customArray = [7];
    Object.setPrototypeOf(customArray, Object.create(Array.prototype));
    const conditions: unknown[] = [
      cyclic,
      { owner: { _eq: Symbol.for('field-policy') } },
      { owner: { _eq: 7 }, [Symbol.for('metadata')]: true },
      { owner: { _in: customArray } },
    ];
    for (const condition of conditions) {
      expect(() => prepareFieldPermissionCondition(condition)).not.toThrow();
      expect(Object.isFrozen(condition)).toBe(false);
    }
  });

  it('prepares shared data graphs once but retains per-call user and record decisions', () => {
    const shared = { _eq: '@USER.id' };
    const condition = { _and: [{ owner: shared }, { reviewer: shared }] };
    prepareFieldPermissionCondition(condition);
    prepareFieldPermissionCondition(condition);
    expect(Object.isFrozen(shared)).toBe(true);
    expect(match(condition, { owner: 7, reviewer: 7 }, { id: 7 })).toBe(true);
    expect(match(condition, { owner: 7, reviewer: 7 }, { id: 8 })).toBe(false);
    expect(match(condition, { owner: 7, reviewer: 8 }, { id: 7 })).toBe(false);
  });

  it('extracts unique nested dependency paths without interpreting operator operands', () => {
    const condition = {
      _and: [
        { owner: { id: { _eq: '@USER.id' } } },
        { _not: { deletedAt: { _is_null: true } } },
        { 'owner.id': { _in: ['@USER.allowed'] } },
        { _or: [{ tenant: { name: { _eq: 'pro' } } }] },
      ],
      _unknown: { ignored: true },
    };
    expect(getFieldPermissionConditionFields(condition)).toEqual([
      'owner.id', 'deletedAt', 'tenant.name',
    ]);
    expect(getFieldPermissionConditionFields(null)).toEqual([]);
    expect(getFieldPermissionConditionFields(['owner'])).toEqual([]);
  });
});

describe('buildRequestedShapeFromQuery', () => {
  it('preserves empty segments and selects only the first dotted field segment', () => {
    const requested = buildRequestedShapeFromQuery({
      fields: ['', '.secret', 'author.name', 'title', '*'],
      deep: null,
    });

    expect(requested.includeAll).toBe(true);
    expect(requested.columns).toEqual(new Set(['', 'author', 'title', '*']));
  });

  it('does not broaden a nested field request to every column', () => {
    const requested = buildRequestedShapeFromQuery({
      fields: 'author.name,title',
      deep: null,
    });

    expect(requested.includeAll).toBe(false);
    expect(requested.columns).toEqual(new Set(['author', 'title']));
  });
});

describe('sanitizeFieldPermissionsResult', () => {
  it('evaluates conditional denies even when a field is published', async () => {
    const policyReader = {
      getFieldPermissionPoliciesFor: () => [{
        unconditionalAllowedColumns: new Set(),
        unconditionalAllowedRelations: new Set(),
        unconditionalDeniedColumns: new Set(),
        unconditionalDeniedRelations: new Set(),
        rules: [{
          id: 'deny-owner',
          isEnabled: true,
          action: 'read',
          effect: 'deny',
          tableName: 'posts',
          roleId: null,
          allowedUserIds: ['user-1'],
          columnName: 'secret',
          relationPropertyName: null,
          condition: { ownerId: { _eq: '@USER.id' } },
        }],
      }],
    };
    const metadata = {
      tables: new Map([['posts', {
        columns: [{ name: 'ownerId', isPublished: true }, { name: 'secret', isPublished: true }],
        relations: [],
      }]]),
    };

    const result = await sanitizeFieldPermissionsResult({
      value: [{ ownerId: 'user-1', secret: 'must-not-leak' }],
      tableName: 'posts',
      user: { id: 'user-1' },
      action: 'read',
      fieldPermissionPolicyReader: policyReader,
      metadata,
      requested: { includeAll: true },
    });

    expect(result).toEqual([{ ownerId: 'user-1' }]);
  });
});
