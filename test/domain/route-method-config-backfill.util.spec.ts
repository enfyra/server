import { describe, expect, it } from 'vitest';
import { buildRouteMethodConfigBackfillDrafts } from '../../src/engines/bootstrap/utils/route-method-config-backfill.util';

describe('route method config backfill drafts', () => {
  it('creates the complete route-method matrix and overlays legacy state', () => {
    const drafts = buildRouteMethodConfigBackfillDrafts({
      routeIds: [1, 2],
      methodIds: [10, 11, 12],
      flags: {
        available: [{ routeId: 1, methodId: 10 }],
        public: [{ routeId: 1, methodId: 11 }],
        skipRoleGuard: [{ routeId: 2, methodId: 12 }],
      },
      handlers: [
        { routeId: 2, methodId: 10, handlerId: 100, timeout: 45_000 },
      ],
      systemRouteIds: [1],
    });

    expect(drafts).toHaveLength(6);
    expect(drafts).toEqual([
      {
        routeId: 1,
        methodId: 10,
        available: true,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 30_000,
        isSystem: true,
      },
      {
        routeId: 1,
        methodId: 11,
        available: false,
        isPublic: true,
        skipRoleGuard: false,
        timeout: 30_000,
        isSystem: true,
      },
      {
        routeId: 1,
        methodId: 12,
        available: false,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 30_000,
        isSystem: true,
      },
      {
        routeId: 2,
        methodId: 10,
        available: false,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 45_000,
        isSystem: false,
        handlerId: 100,
      },
      {
        routeId: 2,
        methodId: 11,
        available: false,
        isPublic: false,
        skipRoleGuard: false,
        timeout: 30_000,
        isSystem: false,
      },
      {
        routeId: 2,
        methodId: 12,
        available: false,
        isPublic: false,
        skipRoleGuard: true,
        timeout: 30_000,
        isSystem: false,
      },
    ]);
  });

  it('does not create duplicate cells when legacy sources overlap', () => {
    const pair = { routeId: 1, methodId: 10 };
    const drafts = buildRouteMethodConfigBackfillDrafts({
      routeIds: [1],
      methodIds: [10],
      flags: {
        available: [pair],
        public: [pair],
        skipRoleGuard: [pair],
      },
      handlers: [{ ...pair, handlerId: 100, timeout: 30_000 }],
    });

    expect(drafts).toHaveLength(1);
    expect(drafts[0]).toMatchObject({
      available: true,
      isPublic: true,
      skipRoleGuard: true,
      handlerId: 100,
    });
  });

  it.each([
    [null, 60_000],
    [undefined, 60_000],
    [0, 60_000],
    [-25, 1],
    [15_900.8, 15_900],
  ])('preserves effective legacy timeout %j as %i', (timeout, expected) => {
    const [draft] = buildRouteMethodConfigBackfillDrafts({
      routeIds: [1],
      methodIds: [10],
      flags: { available: [], public: [], skipRoleGuard: [] },
      handlers: [{ routeId: 1, methodId: 10, handlerId: 1, timeout }],
    });

    expect(draft.timeout).toBe(expected);
  });
});
