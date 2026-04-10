import { ObjectId } from 'mongodb';
import { PolicyService } from '../../src/core/policy/policy.service';

describe('PolicyService.assertSystemSafe — user_definition self-check', () => {
  const makePolicy = () =>
    new PolicyService(
      { assertNoSystemFlagDeep: jest.fn() } as any,
      {
        getMetadata: jest.fn().mockResolvedValue({ tables: new Map() }),
      } as any,
    );

  it('root admin can update themselves (SQL integer id)', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'update',
      tableName: 'user_definition',
      data: { password: 'newpass' },
      existing: { id: 1, isRootAdmin: true },
      currentUser: { id: 1 },
    });
    expect(d.allow).toBe(true);
  });

  it('root admin can update themselves (MongoDB ObjectId)', async () => {
    const policy = makePolicy();
    const hex = '507f1f77bcf86cd799439011';
    const d = await policy.checkMutationSafety({
      operation: 'update',
      tableName: 'user_definition',
      data: { password: 'newpass' },
      existing: { _id: new ObjectId(hex), isRootAdmin: true },
      currentUser: { _id: new ObjectId(hex) },
    });
    expect(d.allow).toBe(true);
  });

  it('non-root admin cannot update root admin', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'update',
      tableName: 'user_definition',
      data: { email: 'hack@test.com' },
      existing: { _id: new ObjectId(), isRootAdmin: true },
      currentUser: { _id: new ObjectId() },
    });
    expect(d.allow).toBe(false);
  });

  it('cannot modify isRootAdmin flag', async () => {
    const policy = makePolicy();
    const hex = '507f1f77bcf86cd799439011';
    const d = await policy.checkMutationSafety({
      operation: 'update',
      tableName: 'user_definition',
      data: { isRootAdmin: false },
      existing: { _id: new ObjectId(hex), isRootAdmin: true },
      currentUser: { _id: new ObjectId(hex) },
    });
    expect(d.allow).toBe(false);
  });
});

describe('PolicyService.checkMutationSafety', () => {
  const makePolicy = () =>
    new PolicyService(
      { assertNoSystemFlagDeep: jest.fn() } as any,
      {
        getMetadata: jest.fn().mockResolvedValue({ tables: new Map() }),
      } as any,
    );

  it('allows create when assertNoSystemFlagDeep passes', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'create',
      tableName: 'custom_table',
      data: { name: 'row1' },
      existing: null,
      currentUser: null,
    });
    expect(d.allow).toBe(true);
  });

  it('blocks delete of system record', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'delete',
      tableName: 'any_table',
      data: null,
      existing: { isSystem: true, id: 1 },
      currentUser: null,
    });
    expect(d.allow).toBe(false);
    expect(d.code).toBe('SYSTEM_PROTECTION');
    expect(d.message).toContain('Cannot delete system record');
  });

  it('blocks create of system hook', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'create',
      tableName: 'pre_hook_definition',
      data: { isSystem: true, code: 'return {}' },
      existing: null,
      currentUser: null,
    });
    expect(d.allow).toBe(false);
    expect(d.code).toBe('SYSTEM_PROTECTION');
    expect(d.message).toContain('Cannot create system hook');
  });

  it('blocks forbidden field change on system route', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'update',
      tableName: 'route_definition',
      data: { path: '/new' },
      existing: { isSystem: true, path: '/old', handlers: [] },
      currentUser: null,
    });
    expect(d.allow).toBe(false);
    expect(d.code).toBe('SYSTEM_PROTECTION');
    expect(d.message).toContain('Cannot modify system route');
  });

  it('allows allowed field change on system route', async () => {
    const policy = makePolicy();
    const d = await policy.checkMutationSafety({
      operation: 'update',
      tableName: 'route_definition',
      data: { description: 'updated' },
      existing: { isSystem: true, description: 'old', handlers: [] },
      currentUser: null,
    });
    expect(d.allow).toBe(true);
  });

  it('maps assertNoSystemFlagDeep failure to SYSTEM_PROTECTION', async () => {
    const common = {
      assertNoSystemFlagDeep: jest.fn(() => {
        throw new Error('nested isSystem');
      }),
    };
    const policy = new PolicyService(
      common as any,
      {
        getMetadata: jest.fn().mockResolvedValue({ tables: new Map() }),
      } as any,
    );
    const d = await policy.checkMutationSafety({
      operation: 'create',
      tableName: 'x',
      data: { a: 1 },
      existing: null,
      currentUser: null,
    });
    expect(d.allow).toBe(false);
    expect(d.code).toBe('SYSTEM_PROTECTION');
    expect(d.message).toBe('nested isSystem');
  });
});
