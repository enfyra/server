import { describe, expect, it } from 'vitest';
import {
  assertGraphqlPermissionScope,
  assertNoPublicPermissionOverlap,
  hasGraphqlOperationAccess,
  normalizeGraphqlOperation,
  normalizeGraphqlOperationList,
} from '../../src/modules/graphql/utils/graphql-access.util';

const baseDefinition = {
  isEnabled: true,
  publicOperations: [] as Array<'QUERY' | 'CREATE' | 'UPDATE' | 'DELETE'>,
  permissions: [],
};

describe('GraphQL access utility', () => {
  it('normalizes canonical operations and rejects unknown values', () => {
    expect(normalizeGraphqlOperation('query')).toBe('QUERY');
    expect(normalizeGraphqlOperation('UPDATE')).toBe('UPDATE');
    expect(() => normalizeGraphqlOperation('PATCH')).toThrow(
      'Unsupported GraphQL operation: PATCH',
    );
  });

  it('normalizes relation objects and deduplicates operation lists', () => {
    expect(
      normalizeGraphqlOperationList([
        'query',
        { name: 'QUERY' },
        { name: 'create' },
      ]),
    ).toEqual(['QUERY', 'CREATE']);
  });

  it('requires exactly one permission scope', () => {
    expect(() =>
      assertGraphqlPermissionScope({ role: { id: 'role-1' }, allowedUsers: [] }),
    ).not.toThrow();
    expect(() =>
      assertGraphqlPermissionScope({ role: null, allowedUsers: [{ id: 'user-1' }] }),
    ).not.toThrow();
    expect(() =>
      assertGraphqlPermissionScope({ role: null, allowedUsers: [] }),
    ).toThrow('GraphQL permission must target exactly one scope');
    expect(() =>
      assertGraphqlPermissionScope({
        role: { id: 'role-1' },
        allowedUsers: [{ id: 'user-1' }],
      }),
    ).toThrow('GraphQL permission must target exactly one scope');
  });

  it('rejects public and permission operation overlap', () => {
    expect(() =>
      assertNoPublicPermissionOverlap({
        publicOperations: ['QUERY', 'CREATE'],
        permissionOperations: ['UPDATE'],
      }),
    ).not.toThrow();
    expect(() =>
      assertNoPublicPermissionOverlap({
        publicOperations: ['QUERY'],
        permissionOperations: ['QUERY', 'UPDATE'],
      }),
    ).toThrow(
      'Public GraphQL operations cannot also be granted by permissions: QUERY',
    );
  });

  it('allows public operations without a user', () => {
    expect(
      hasGraphqlOperationAccess({
        definition: { ...baseDefinition, publicOperations: ['QUERY'] },
        operation: 'QUERY',
        user: null,
      }),
    ).toEqual({ allowed: true, isPublic: true });
  });

  it('default-denies disabled definitions, anonymous users, and unmatched grants', () => {
    expect(
      hasGraphqlOperationAccess({
        definition: { ...baseDefinition, isEnabled: false },
        operation: 'QUERY',
        user: { id: 'user-1', isRootAdmin: true },
      }).allowed,
    ).toBe(false);
    expect(
      hasGraphqlOperationAccess({
        definition: baseDefinition,
        operation: 'QUERY',
        user: null,
      }).allowed,
    ).toBe(false);
    expect(
      hasGraphqlOperationAccess({
        definition: baseDefinition,
        operation: 'QUERY',
        user: { id: 'user-1', role: { id: 'role-1' } },
      }).allowed,
    ).toBe(false);
  });

  it('allows root, matching role, and matching explicit-user grants only', () => {
    expect(
      hasGraphqlOperationAccess({
        definition: baseDefinition,
        operation: 'DELETE',
        user: { id: 'root-1', isRootAdmin: true },
      }).allowed,
    ).toBe(true);

    const definition = {
      ...baseDefinition,
      permissions: [
        {
          isEnabled: true,
          roleId: 'role-editor',
          allowedUserIds: [],
          operations: ['UPDATE'] as const,
        },
        {
          isEnabled: true,
          roleId: null,
          allowedUserIds: ['user-special'],
          operations: ['DELETE'] as const,
        },
        {
          isEnabled: false,
          roleId: 'role-editor',
          allowedUserIds: [],
          operations: ['CREATE'] as const,
        },
      ],
    };

    expect(
      hasGraphqlOperationAccess({
        definition,
        operation: 'UPDATE',
        user: { id: 'user-1', role: { id: 'role-editor' } },
      }).allowed,
    ).toBe(true);
    expect(
      hasGraphqlOperationAccess({
        definition,
        operation: 'DELETE',
        user: { _id: 'user-special', role: null },
      }).allowed,
    ).toBe(true);
    expect(
      hasGraphqlOperationAccess({
        definition,
        operation: 'CREATE',
        user: { id: 'user-1', role: { id: 'role-editor' } },
      }).allowed,
    ).toBe(false);
  });
});
