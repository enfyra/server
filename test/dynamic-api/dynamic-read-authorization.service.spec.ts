import { describe, expect, it } from 'vitest';
import { DynamicReadAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-read-authorization.service';
import type { RuntimeRegistryService } from '../../src/engines/cache/services/runtime-registry.service';

function createService() {
  const runtimeRegistryService = {
    requireMetadata: () => ({
      tables: new Map([
        [
          'articles',
          {
            columns: [
              { name: 'id', isEncrypted: false },
              { name: 'secret', isEncrypted: true },
            ],
            relations: [
              {
                propertyName: 'author',
                targetTableName: 'authors',
              },
            ],
          },
        ],
        [
          'authors',
          {
            columns: [{ name: 'email', isEncrypted: true }],
            relations: [],
          },
        ],
      ]),
      tablesList: [],
      version: 1,
      timestamp: new Date(),
    }),
  };
  return new DynamicReadAuthorizationService({
    runtimeRegistryService: runtimeRegistryService as RuntimeRegistryService,
  });
}

describe('DynamicReadAuthorizationService', () => {
  it('rejects an encrypted root filter', async () => {
    await expect(
      createService().assertEncryptedQueryFieldsAllowed(
        'articles',
        { secret: { _eq: 'value' } },
        undefined,
        {},
      ),
    ).rejects.toThrow("Encrypted field 'secret' on 'articles' cannot be used for filter.");
  });

  it('rejects an encrypted nested sort', async () => {
    await expect(
      createService().assertEncryptedQueryFieldsAllowed(
        'articles',
        undefined,
        undefined,
        { author: { sort: '-email' } },
      ),
    ).rejects.toThrow("Encrypted field 'email' on 'authors' cannot be used for sort.");
  });

  it('rejects a filter for an unpublished field without a read policy', async () => {
    const runtimeRegistryService = {
      lookupTableByName: () => ({
        columns: [{ name: 'internalStatus', isPublished: false }],
        relations: [],
      }),
      getFieldPermissionPoliciesFor: () => [],
    };
    const service = new DynamicReadAuthorizationService({
      runtimeRegistryService: runtimeRegistryService as RuntimeRegistryService,
    });

    await expect(
      service.assertQueryAllowed({
        tableName: 'articles',
        context: {
          $user: { id: 'reader' },
          $query: { filter: { internalStatus: { _eq: 'draft' } } },
        },
        enforceFieldPermission: true,
      }),
    ).rejects.toThrow(
      "You do not have permission to filter column 'internalStatus' on table 'articles'.",
    );
  });

  it('removes an unpublished projection field when no read policy allows it', async () => {
    const runtimeRegistryService = {
      lookupTableByName: () => ({
        columns: [
          { name: 'title', isPublished: true },
          { name: 'internalStatus', isPublished: false },
        ],
        relations: [],
      }),
      getFieldPermissionPoliciesFor: () => [],
      requireMetadata: () => ({
        tables: new Map([
          [
            'articles',
            {
              columns: [
                { name: 'title', isPublished: true },
                { name: 'internalStatus', isPublished: false },
              ],
              relations: [],
            },
          ],
        ]),
        tablesList: [],
        version: 1,
        timestamp: new Date(),
      }),
    };
    const service = new DynamicReadAuthorizationService({
      runtimeRegistryService: runtimeRegistryService as RuntimeRegistryService,
    });

    await expect(
      service.stripDeniedFields({
        tableName: 'articles',
        fields: 'title,internalStatus',
        deep: undefined,
        context: { $user: { id: 'reader' } },
        enforceFieldPermission: true,
      }),
    ).resolves.toEqual({
      fields: 'title',
      deep: undefined,
      needsPostSql: false,
    });
  });
});
