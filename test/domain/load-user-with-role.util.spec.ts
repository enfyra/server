import { ObjectId } from 'mongodb';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { DatabaseConfigService } from '../../src/shared/services';
import { loadUserWithRole } from '../../src/shared/utils/load-user-with-role.util';

describe('loadUserWithRole', () => {
  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('returns null for invalid Mongo user ids without querying', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const queryBuilder = {
      isMongoDb: () => true,
      findOne: vi.fn(),
    } as any;

    await expect(loadUserWithRole(queryBuilder, 'admin')).resolves.toBeNull();

    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('queries Mongo users and roles with ObjectId values', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const userId = new ObjectId();
    const roleId = new ObjectId();
    const role = { _id: roleId, name: 'Admin' };
    const user = { _id: userId, email: 'root@example.com', role: roleId };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => true,
      findOne,
    } as any;

    const result = await loadUserWithRole(queryBuilder, userId.toHexString());

    expect(findOne).toHaveBeenNthCalledWith(1, {
      table: 'enfyra_user',
      where: { _id: userId },
    });
    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'enfyra_role',
      where: { _id: roleId },
    });
    expect(result?.role).toEqual(role);
  });

  it('returns null for invalid SQL uuid-like ids without querying', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const queryBuilder = {
      isMongoDb: () => false,
      findOne: vi.fn(),
    } as any;

    await expect(
      loadUserWithRole(queryBuilder, '69f21541e94cdbc8666b1a52'),
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
      loadUserWithRole(queryBuilder, new ObjectId()),
    ).resolves.toBeNull();

    expect(queryBuilder.findOne).not.toHaveBeenCalled();
  });

  it('queries SQL users with UUID ids and integer role ids', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const userId = '6dcaf98d-07a0-4d7e-88ad-87dd1e3b113d';
    const role = { id: 1, name: 'Admin' };
    const user = { id: userId, email: 'root@example.com', roleId: 1 };
    const findOne = vi.fn(async ({ table }) => {
      if (table === 'enfyra_user') return user;
      if (table === 'enfyra_role') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => false,
      findOne,
    } as any;

    const result = await loadUserWithRole(queryBuilder, userId);

    expect(findOne).toHaveBeenNthCalledWith(1, {
      table: 'enfyra_user',
      where: { id: userId },
    });
    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'enfyra_role',
      where: { id: 1 },
    });
    expect(result?.role).toEqual(role);
  });
});
