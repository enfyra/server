import { describe, expect, it, vi } from 'vitest';
import { stripUnauthorizedMutationFields } from '../../src/shared/utils/strip-unauthorized-mutation-fields.util';

const tableMeta = {
  columns: [
    { name: 'title', isPublished: true },
    { name: 'privateValue', isPublished: false },
    { name: 'encryptedSecret', isPublished: false, isEncrypted: true },
  ],
  relations: [{ propertyName: 'privateOwner', isPublished: false }],
};

function policy(action: 'create' | 'update', field: string, effect: 'allow' | 'deny', condition: any = null) {
  return {
    unconditionalAllowedColumns: new Set(),
    unconditionalAllowedRelations: new Set(),
    unconditionalDeniedColumns: new Set(),
    unconditionalDeniedRelations: new Set(),
    rules: [
      {
        id: `${effect}-${action}-${field}`,
        isEnabled: true,
        action,
        effect,
        tableName: 'articles',
        roleId: null,
        allowedUserIds: ['editor'],
        columnName: field,
        relationPropertyName: null,
        condition,
      },
    ],
  };
}

describe('stripUnauthorizedMutationFields', () => {
  it('keeps every supplied field for a root administrator', async () => {
    const getFieldPermissionPoliciesFor = vi.fn(() => []);
    const body = {
      title: 'public',
      privateValue: null,
      encryptedSecret: '',
      privateOwner: { id: 7 },
    };

    await expect(
      stripUnauthorizedMutationFields({
        action: 'create',
        body,
        policyReader: { getFieldPermissionPoliciesFor },
        record: body,
        tableMeta,
        tableName: 'articles',
        user: { id: 'root', isRootAdmin: true },
      }),
    ).resolves.toEqual(body);
    expect(getFieldPermissionPoliciesFor).not.toHaveBeenCalled();
  });

  it('silently strips every denied field while retaining allowed and unknown input', async () => {
    const body = {
      title: 'public',
      privateValue: 'hidden',
      encryptedSecret: 'secret',
      privateOwner: { id: 7 },
      unknownClientField: 'validated later',
    };

    const result = await stripUnauthorizedMutationFields({
      action: 'create',
      body,
      policyReader: { getFieldPermissionPoliciesFor: () => [] },
      record: body,
      tableMeta,
      tableName: 'articles',
      user: { id: 'editor' },
    });

    expect(result).toEqual({
      title: 'public',
      unknownClientField: 'validated later',
    });
    expect(body).toHaveProperty('privateValue', 'hidden');
  });

  it('keeps a private field with an explicit matching permission', async () => {
    const body = { privateValue: null };

    await expect(
      stripUnauthorizedMutationFields({
        action: 'update',
        body,
        policyReader: {
          getFieldPermissionPoliciesFor: () => [
            policy('update', 'privateValue', 'allow'),
          ],
        },
        record: { id: 1, ownerId: 'editor' },
        tableMeta,
        tableName: 'articles',
        user: { id: 'editor' },
      }),
    ).resolves.toEqual(body);
  });

  it('strips a published field when an explicit permission denies it', async () => {
    await expect(
      stripUnauthorizedMutationFields({
        action: 'update',
        body: { title: 'blocked' },
        policyReader: {
          getFieldPermissionPoliciesFor: () => [
            policy('update', 'title', 'deny'),
          ],
        },
        record: { id: 1 },
        tableMeta,
        tableName: 'articles',
        user: { id: 'editor' },
      }),
    ).resolves.toEqual({});
  });

  it('evaluates conditional permissions against the persisted update record', async () => {
    const conditional = policy('update', 'privateValue', 'allow', {
      ownerId: { _eq: '@USER.id' },
    });
    const base = {
      action: 'update' as const,
      body: { privateValue: 'next' },
      policyReader: { getFieldPermissionPoliciesFor: () => [conditional] },
      tableMeta,
      tableName: 'articles',
      user: { id: 'editor' },
    };

    await expect(
      stripUnauthorizedMutationFields({
        ...base,
        record: { id: 1, ownerId: 'editor' },
      }),
    ).resolves.toEqual({ privateValue: 'next' });
    await expect(
      stripUnauthorizedMutationFields({
        ...base,
        record: { id: 1, ownerId: 'someone-else' },
      }),
    ).resolves.toEqual({});
  });
});
