import { ObjectId } from 'mongodb';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DatabaseConfigService } from '../../src/shared/services';
import {
  clearLocalUserCacheForTesting,
  loadCachedUserWithRoles,
  loadUserWithRoles,
  primeCachedUserSnapshot,
} from '../../src/shared/utils/load-user-with-role.util';

function makeAuthorizationCache(revision = 'test') {
  const values = new Map<string, unknown>();
  return {
    get: vi.fn(async (key: string) => values.get(key) ?? revision),
    set: vi.fn(async (key: string, value: unknown) => {
      values.set(key, value);
    }),
    setRevision(value: string) {
      revision = value;
    },
  } as any;
}

describe('loadUserWithRoles', () => {
  afterEach(() => {
    DatabaseConfigService.resetForTesting();
    clearLocalUserCacheForTesting();
  });

  it('returns null for invalid Mongo user ids without querying', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const queryBuilder = {
      isMongoDb: () => true,
      findOne: vi.fn(),
    } as any;

    await expect(loadUserWithRoles(queryBuilder, 'admin')).resolves.toBeNull();

    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('queries Mongo users and every assigned role with ObjectId values', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const userId = new ObjectId();
    const roleId = new ObjectId();
    const moderatorRoleId = new ObjectId();
    const role = { _id: roleId, name: 'Member' };
    const moderatorRole = { _id: moderatorRoleId, name: 'Moderator' };
    const user = {
      _id: userId,
      email: 'root@example.com',
      roles: [roleId, moderatorRoleId],
    };
    const findOne = vi.fn(async ({ table, where }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') {
        return roleId.equals(where?._id) ? role : moderatorRole;
      }
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => true,
      findOne,
    } as any;

    const result = await loadUserWithRoles(queryBuilder, userId.toHexString());

    expect(findOne).toHaveBeenNthCalledWith(1, {
      table: 'enfyra_user',
      where: { _id: userId },
      fields: ['*', 'roles.*'],
    });
    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'enfyra_role',
      where: { _id: roleId },
    });
    expect(findOne).toHaveBeenNthCalledWith(3, {
      table: 'enfyra_role',
      where: { _id: moderatorRoleId },
    });
    expect(result?.roles).toEqual([role, moderatorRole]);
  });

  it('returns null for invalid SQL uuid-like ids without querying', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const queryBuilder = {
      isMongoDb: () => false,
      findOne: vi.fn(),
    } as any;

    await expect(
      loadUserWithRoles(queryBuilder, '69f21541e94cdbc8666b1a52'),
    ).resolves.toBeNull();

    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('returns null for SQL ObjectId values without querying', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const queryBuilder = {
      isMongoDb: () => false,
      findOne: vi.fn(),
    } as any;

    await expect(
      loadUserWithRoles(queryBuilder, new ObjectId()),
    ).resolves.toBeNull();

    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('queries SQL users with UUID ids and integer role ids', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const userId = '6dcaf98d-07a0-4d7e-88ad-87dd1e3b113d';
    const role = { id: 1, name: 'Admin' };
    const user = { id: userId, email: 'root@example.com', roles: [{ id: 1 }] };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => false,
      findOne,
    } as any;

    const result = await loadUserWithRoles(queryBuilder, userId);

    expect(findOne).toHaveBeenNthCalledWith(1, {
      table: 'enfyra_user',
      where: { id: userId },
      fields: ['*', 'roles.*'],
    });
    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'enfyra_role',
      where: { id: 1 },
    });
    expect(result?.roles).toEqual([role]);
  });

  it('hydrates full SQL roles from wildcard relation stubs', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const user = {
      id: '6dcaf98d-07a0-4d7e-88ad-87dd1e3b113d',
      email: 'member@example.com',
      roles: [{ id: 2 }],
    };
    const role = { id: 2, name: 'Member' };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => false,
      findOne,
    } as any;

    const result = await loadUserWithRoles(queryBuilder, user.id);

    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'enfyra_role',
      where: { id: 2 },
    });
    expect(result?.roles).toEqual([role]);
  });

  it('returns local cached users without querying the database', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const cachedUser = { id: '1', email: 'cached@example.com' };
    primeCachedUserSnapshot('1', cachedUser);
    const cacheService = makeAuthorizationCache();
    const queryBuilder = {
      isMongoDb: () => false,
      findOne: vi.fn(),
    } as any;

    await expect(
      loadCachedUserWithRoles(queryBuilder, cacheService, '1'),
    ).resolves.toBe(cachedUser);

    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('caches loaded users in the local process for one minute', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const user = { id: '1', email: 'root@example.com', roles: [{ id: 2 }] };
    const role = { id: 2, name: 'Admin' };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => false,
      findOne,
    } as any;
    const cacheService = makeAuthorizationCache();

    const result = await loadCachedUserWithRoles(
      queryBuilder,
      cacheService,
      '1',
    );
    const cachedResult = await loadCachedUserWithRoles(
      queryBuilder,
      cacheService,
      '1',
    );

    expect(result?.roles).toEqual([role]);
    expect(cachedResult).toBe(result);
    expect(findOne).toHaveBeenCalledTimes(2);
  });

  it('rehydrates when the shared authorization revision changes without local invalidation', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const user = { id: '1', email: 'member@example.com', roles: [{ id: 2 }] };
    const role = { id: 2, name: 'Member' };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return { ...user, roles: [...user.roles] };
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => false,
      findOne,
    } as any;
    const cacheService = makeAuthorizationCache('before');

    await loadCachedUserWithRoles(queryBuilder, cacheService, '1');
    cacheService.setRevision('after');
    await loadCachedUserWithRoles(queryBuilder, cacheService, '1');

    expect(findOne).toHaveBeenCalledTimes(4);
  });
});
