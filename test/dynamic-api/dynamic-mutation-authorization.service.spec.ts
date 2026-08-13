import { describe, expect, it, vi } from 'vitest';
import { DynamicMutationAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-mutation-authorization.service';
import type { PolicyService } from '../../src/domain/policy';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';

function createService({
  enforceFieldPermission = true,
  table = {
    columns: [{ name: 'id', isPrimary: true }, { name: 'internal', isPublished: false }],
    relations: [],
  },
} = {}) {
  const queryBuilderService = {
    runWithPolicy: vi.fn(async (_check: unknown, callback: () => unknown) =>
      callback(),
    ),
    runWithFieldPermissionCheck: vi.fn(
      async (_check: unknown, callback: () => unknown) => callback(),
    ),
  };
  const policyService = {
    checkMutationSafety: vi.fn().mockResolvedValue({ allowed: true }),
  };
  const runtimeRegistryService = {
    lookupTableByName: vi.fn().mockResolvedValue(table),
    getFieldPermissionPoliciesFor: vi.fn(() => []),
  };
  const service = new DynamicMutationAuthorizationService({
    context: { $user: { id: 'editor' } } as any,
    enforceFieldPermission,
    policyService: policyService as PolicyService,
    queryBuilderService: queryBuilderService as QueryBuilderService,
    runtimeRegistryService: runtimeRegistryService as RuntimeRegistryService,
    tableName: 'articles',
  });
  return { policyService, queryBuilderService, service };
}

describe('DynamicMutationAuthorizationService', () => {
  it('rejects direct writes to unpublished fields without an allowed policy', async () => {
    const { service } = createService();

    await expect(
      service.assertDirectFieldPermission('update', { internal: 'private' }, {}),
    ).rejects.toThrow(
      "You do not have permission to update column 'internal' on table 'articles'.",
    );
  });

  it('runs nested writes through mutation policy and field-permission boundaries', async () => {
    const { queryBuilderService, service } = createService({
      enforceFieldPermission: false,
    });

    await expect(
      service.runWithFieldPermissionCheck(() =>
        service.runWithMutationPolicy(async () => 'created'),
      ),
    ).resolves.toBe('created');

    expect(queryBuilderService.runWithFieldPermissionCheck).not.toHaveBeenCalled();
    expect(queryBuilderService.runWithPolicy).toHaveBeenCalledTimes(1);
  });
});
