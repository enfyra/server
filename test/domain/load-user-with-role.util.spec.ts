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
      if (table === 'user_definition') return user;
      if (table === 'role_definition') return role;
      return null;
    });
    const queryBuilder = {
      isMongoDb: () => true,
      findOne,
    } as any;

    const result = await loadUserWithRole(queryBuilder, userId.toHexString());

    expect(findOne).toHaveBeenNthCalledWith(1, {
      table: 'user_definition',
      where: { _id: userId },
    });
    expect(findOne).toHaveBeenNthCalledWith(2, {
      table: 'role_definition',
      where: { _id: roleId },
    });
    expect(result?.role).toEqual(role);
  });
});
