import { describe, expect, it } from 'vitest';
import { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';
import type {
  TCompiledFieldPolicy,
  TFieldPermissionRule,
} from '../../src/engines/cache/services/field-permission-cache-builder.service';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';
import { decideFieldPermission } from '../../src/shared/utils/field-permission.util';

function policies(condition: unknown): Map<string, TCompiledFieldPolicy> {
  const rule: TFieldPermissionRule = {
    id: 'conditional-deny',
    isEnabled: true,
    action: 'read',
    effect: 'deny',
    tableName: 'posts',
    roleId: null,
    allowedUserIds: [],
    columnName: 'secret',
    relationPropertyName: null,
    condition,
  };
  return new Map([
    [
      'r:null|posts|read',
      {
        unconditionalAllowedColumns: new Set<string>(),
        unconditionalAllowedRelations: new Set<string>(),
        unconditionalDeniedColumns: new Set<string>(),
        unconditionalDeniedRelations: new Set<string>(),
        rules: [rule],
      },
    ],
  ]);
}

const context = {
  user: { id: 7 },
  tableName: 'posts',
  action: 'read' as const,
  subjectType: 'column' as const,
  subjectName: 'secret',
  record: { owner: 7 },
};

describe('field permission snapshot preparation', () => {
  it('prepares only the staged clone and replaces plans on activation', async () => {
    const registry = new RuntimeRegistryService();
    const initial = policies({ owner: { _eq: '@USER.id' } });
    await registry.publishFromCache(CACHE_IDENTIFIERS.FIELD_PERMISSION, {
      getRawCache: () => initial,
    });
    expect((await decideFieldPermission(registry, context)).allowed).toBe(
      false,
    );
    expect(
      (await decideFieldPermission(registry, { ...context, user: { id: 8 } }))
        .allowed,
    ).toBe(true);
    const active = registry.getFieldPermissionPoliciesFor(
      context.user,
      'posts',
      'read',
    )[0].rules[0].condition;
    expect(Object.isFrozen(active)).toBe(true);
    expect(
      Object.isFrozen(initial.values().next().value!.rules[0].condition),
    ).toBe(false);
    const replacement = structuredClone(policies({ owner: { _eq: 8 } }));
    const staged = await registry.stageSnapshotFromCache(
      CACHE_IDENTIFIERS.FIELD_PERMISSION,
      { getRawCache: () => replacement },
    );
    expect((await decideFieldPermission(registry, context)).allowed).toBe(
      false,
    );
    registry.activateSnapshots([staged]);
    expect((await decideFieldPermission(registry, context)).allowed).toBe(true);
    expect(
      (
        await decideFieldPermission(registry, {
          ...context,
          record: { owner: 8 },
        })
      ).allowed,
    ).toBe(false);
    expect((await decideFieldPermission(registry, context)).allowed).toBe(true);
  });

  it('retains the old prepared deny when a later snapshot fails to build', async () => {
    const registry = new RuntimeRegistryService();
    await registry.publishFromCache(CACHE_IDENTIFIERS.FIELD_PERMISSION, {
      getRawCache: () => policies({ owner: { _eq: '@USER.id' } }),
    });
    await expect(
      registry.publishFromCache(CACHE_IDENTIFIERS.FIELD_PERMISSION, {
        getCacheAsync: async () => {
          throw new Error('candidate failed');
        },
      }),
    ).rejects.toThrow('candidate failed');
    expect((await decideFieldPermission(registry, context)).allowed).toBe(
      false,
    );
    expect(
      (await decideFieldPermission(registry, { ...context, user: { id: 8 } }))
        .allowed,
    ).toBe(true);
  });
});
