import { describe, it, expect } from 'vitest';
import { matchFieldPermissionCondition as match } from '../../src/shared/utils/field-permission-condition.util';

describe('matchFieldPermissionCondition', () => {
  describe('shape / safety', () => {
    it('returns false for non-object condition', () => {
      expect(match(null as any, {}, {})).toBe(false);
      expect(match(undefined as any, {}, {})).toBe(false);
      expect(match('x' as any, {}, {})).toBe(false);
      expect(match([{ a: 1 }] as any, {}, {})).toBe(false);
    });

    it('returns true for empty condition (degenerate accept)', () => {
      expect(match({}, { id: 1 }, { id: 1 })).toBe(true);
    });

    it('rejects unknown operator key at root (underscore prefix)', () => {
      expect(match({ _bogus: 1 } as any, { id: 1 }, {})).toBe(false);
    });

    it('rejects unknown operator inside field node', () => {
      expect(match({ id: { _foo: 1 } } as any, { id: 1 }, {})).toBe(false);
    });
  });

  describe('_eq / _neq', () => {
    it('matches id equality via macro (@USER.id)', () => {
      expect(
        match({ id: { _eq: '@USER.id' } }, { id: 5 }, { id: 5 }),
      ).toBe(true);
      expect(
        match({ id: { _eq: '@USER.id' } }, { id: '5' }, { id: 5 }),
      ).toBe(true);
      expect(
        match({ id: { _eq: '@USER.id' } }, { id: 5 }, { id: 6 }),
      ).toBe(false);
    });

    it('matches id via _id fallback (mongo record)', () => {
      expect(
        match({ id: { _eq: '@USER.id' } }, { _id: 'abc' }, { _id: 'abc' }),
      ).toBe(true);
    });

    it('_neq flips', () => {
      expect(
        match({ id: { _neq: '@USER.id' } }, { id: 5 }, { id: 6 }),
      ).toBe(true);
      expect(
        match({ id: { _neq: '@USER.id' } }, { id: 5 }, { id: 5 }),
      ).toBe(false);
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
        match({ status: { _in: ['draft', 'review'] } }, { status: 'draft' }, {}),
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
      expect(match({ _and: [] } as any, {}, {})).toBe(false);
      expect(match({ _or: [] } as any, {}, {})).toBe(false);
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
      expect(match({ _not: null } as any, {}, {})).toBe(false);
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
      expect(
        match(
          { owner: { id: { _eq: '@USER.id' } } },
          {},
          { id: 5 },
        ),
      ).toBe(false);
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
        match(
          { owner: { id: { _eq: '@USER.id' } } },
          { owner: 5 },
          { id: 5 },
        ),
      ).toBe(false);
    });
  });

  describe('@USER macro paths', () => {
    it('@USER and @USER.id / @USER._id', () => {
      expect(
        match({ id: { _eq: '@USER' } }, { id: 5 }, { id: 5 }),
      ).toBe(true);
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
        match(
          { roleId: { _eq: '@USER.role.id' } },
          { roleId: 7 },
          { id: 1 },
        ),
      ).toBe(false);
    });

    it('@USER macro only in value, not in field key', () => {
      expect(
        match(
          { '@USER.id': { _eq: 1 } } as any,
          { '@USER.id': 1 },
          {},
        ),
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
      expect(
        match(cond, { ownerId: 5, status: 'draft' }, { id: 5 }),
      ).toBe(true);
      expect(
        match(cond, { ownerId: 5, status: 'published' }, { id: 5 }),
      ).toBe(false);
      expect(
        match(cond, { ownerId: 6, status: 'draft' }, { id: 5 }),
      ).toBe(false);
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
