import { describe, expect, it, vi } from 'vitest';
import { DynamicMutationAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-mutation-authorization.service';
import type { PolicyService } from '../../src/domain/policy';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';

function createService({
  enforceFieldPermission = true,
  policies = [],
  table = {
    columns: [{ name: 'id', isPrimary: true }, { name: 'internal', isPublished: false }],
    relations: [],
  },
  user = { id: 'editor' },
}: {
  enforceFieldPermission?: boolean;
  policies?: any[];
  table?: any;
  user?: any;
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
    getFieldPermissionPoliciesFor: vi.fn(() => policies),
  };
  const service = new DynamicMutationAuthorizationService({
    context: { $user: user } as any,
    enforceFieldPermission,
    policyService: policyService as PolicyService,
    queryBuilderService: queryBuilderService as QueryBuilderService,
    runtimeRegistryService: runtimeRegistryService as RuntimeRegistryService,
    tableName: 'articles',
  });
  return { policyService, queryBuilderService, runtimeRegistryService, service };
}

describe('DynamicMutationAuthorizationService', () => {
  it('silently strips direct writes without an allowed field permission', async () => {
    const { service } = createService();

    await expect(
      service.stripUnauthorizedDirectFields('create', {
        internal: 'private',
      }),
    ).resolves.toEqual({});
    await expect(
      service.stripUnauthorizedDirectFields(
        'update',
        { internal: 'private' },
        {},
      ),
    ).resolves.toEqual({});
  });

  it('allows a root administrator to create and update an unpublished field', async () => {
    const { runtimeRegistryService, service } = createService({
      user: { id: 'root', isRootAdmin: true },
    });

    await expect(
      service.stripUnauthorizedDirectFields('create', { internal: '' }),
    ).resolves.toEqual({ internal: '' });
    await expect(
      service.stripUnauthorizedDirectFields('update', { internal: null }, {}),
    ).resolves.toEqual({ internal: null });
    expect(runtimeRegistryService.lookupTableByName).not.toHaveBeenCalled();
  });

  it('allows unpublished field writes through explicit field permissions', async () => {
    const { service } = createService({
      policies: [
        {
          unconditionalAllowedColumns: new Set(['internal']),
          unconditionalAllowedRelations: new Set(),
          unconditionalDeniedColumns: new Set(),
          unconditionalDeniedRelations: new Set(),
          rules: [
            {
              id: 'allow-editor-internal-create',
              isEnabled: true,
              action: 'create',
              effect: 'allow',
              tableName: 'articles',
              roleId: null,
              allowedUserIds: ['editor'],
              columnName: 'internal',
              relationPropertyName: null,
              condition: null,
            },
            {
              id: 'allow-editor-internal-update',
              isEnabled: true,
              action: 'update',
              effect: 'allow',
              tableName: 'articles',
              roleId: null,
              allowedUserIds: ['editor'],
              columnName: 'internal',
              relationPropertyName: null,
              condition: null,
            },
          ],
        },
      ],
    });

    await expect(
      service.stripUnauthorizedDirectFields('create', { internal: '' }),
    ).resolves.toEqual({ internal: '' });
    await expect(
      service.stripUnauthorizedDirectFields('update', { internal: null }, {}),
    ).resolves.toEqual({ internal: null });
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

  it('silently strips unauthorized fields from nested writes', async () => {
    const { queryBuilderService, service } = createService();
    const nestedBody = { internal: 'must-not-persist', unknown: 'keep' };
    queryBuilderService.runWithFieldPermissionCheck.mockImplementationOnce(
      async (check: any, callback: () => Promise<unknown>) => {
        await check('articles', 'create', nestedBody);
        return callback();
      },
    );

    await expect(
      service.runWithFieldPermissionCheck(async () => 'created'),
    ).resolves.toBe('created');
    expect(nestedBody).toEqual({ unknown: 'keep' });
  });
});
