import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { GqlDefinitionCacheService } from '../../src/engines/cache/services/gql-definition-cache.service';

function makeService() {
  return new GqlDefinitionCacheService({
    queryBuilderService: { find: vi.fn() } as any,
    eventEmitter: new EventEmitter2(),
  });
}

function transform(rows: any[]) {
  return (makeService() as any).transformData(rows);
}

const validRow = {
  id: 42,
  isEnabled: true,
  isSystem: false,
  description: 'Posts GraphQL',
  metadata: { source: 'test' },
  table: { id: 7, name: 'posts' },
  publicOperations: [{ name: 'QUERY' }],
  permissions: [
    {
      id: 9,
      isEnabled: true,
      role: { id: 3 },
      allowedUsers: [],
      operations: [{ name: 'UPDATE' }],
    },
    {
      id: 10,
      isEnabled: false,
      role: null,
      allowedUsers: [{ _id: 'user-1' }],
      operations: [{ name: 'DELETE' }],
    },
  ],
};

describe('GqlDefinitionCacheService authorization snapshot', () => {
  it('builds one atomic normalized access definition', () => {
    const cache = transform([validRow]);

    expect(cache.get('posts')).toEqual({
      id: '42',
      isEnabled: true,
      isSystem: false,
      description: 'Posts GraphQL',
      metadata: { source: 'test' },
      tableName: 'posts',
      publicOperations: ['QUERY'],
      permissions: [
        {
          isEnabled: true,
          roleId: '3',
          allowedUserIds: [],
          operations: ['UPDATE'],
        },
        {
          isEnabled: false,
          roleId: null,
          allowedUserIds: ['user-1'],
          operations: ['DELETE'],
        },
      ],
    });
  });

  it('fails closed on an invalid operation', () => {
    expect(() =>
      transform([{ ...validRow, publicOperations: [{ name: 'PATCH' }] }]),
    ).toThrow('Unsupported GraphQL operation: PATCH');
  });

  it('fails closed when a permission has no scope', () => {
    expect(() =>
      transform([
        {
          ...validRow,
          publicOperations: [],
          permissions: [
            {
              isEnabled: true,
              role: null,
              allowedUsers: [],
              operations: [{ name: 'QUERY' }],
            },
          ],
        },
      ]),
    ).toThrow('GraphQL permission must target exactly one scope');
  });

  it('fails closed when a permission has both role and explicit-user scopes', () => {
    expect(() =>
      transform([
        {
          ...validRow,
          publicOperations: [],
          permissions: [
            {
              isEnabled: true,
              role: { id: 'role-1' },
              allowedUsers: [{ id: 'user-1' }],
              operations: [{ name: 'QUERY' }],
            },
          ],
        },
      ]),
    ).toThrow('GraphQL permission must target exactly one scope');
  });

  it('fails closed on an empty operation grant', () => {
    expect(() =>
      transform([
        {
          ...validRow,
          publicOperations: [],
          permissions: [
            {
              isEnabled: true,
              role: { id: 'role-1' },
              allowedUsers: [],
              operations: [],
            },
          ],
        },
      ]),
    ).toThrow('GraphQL permission must grant at least one operation');
  });

  it('fails closed on public and permission overlap', () => {
    expect(() =>
      transform([
        {
          ...validRow,
          permissions: [
            {
              isEnabled: true,
              role: { id: 'role-1' },
              allowedUsers: [],
              operations: [{ name: 'QUERY' }],
            },
          ],
        },
      ]),
    ).toThrow(
      'Public GraphQL operations cannot also be granted by permissions: QUERY',
    );
  });
});
