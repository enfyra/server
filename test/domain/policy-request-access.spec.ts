import { ObjectId } from 'mongodb';
import { PolicyService } from '../../src/domain/policy';

describe('PolicyService.checkRequestAccess', () => {
  const policy = new PolicyService({} as any);

  it.each([null, false, 'permission', [], { role: { id: { nested: true } } }])(
    'denies malformed permission metadata %j',
    (permission: unknown) => {
      const decision = policy.checkRequestAccess({
        method: 'GET',
        user: { id: 'user-1', roles: [{ id: 'role-1' }] },
        routeData: { routePermissions: [permission] },
      });
      expect(decision).toMatchObject({ allow: false, statusCode: 403 });
    },
  );

  it('does not call methods from malformed allowedUsers metadata', () => {
    let called = false;
    const decision = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 'user-1', roles: [] },
      routeData: {
        routePermissions: [{
          allowedUsers: { some: () => { called = true; return true; } },
        }],
      },
    });
    expect(called).toBe(false);
    expect(decision).toMatchObject({ allow: false, statusCode: 403 });
  });

  it('matches SQL allowed-user identities across string and numeric projections', () => {
    const decision = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 42, roles: [] },
      routeData: { routePermissions: [{ allowedUsers: [{ id: '42' }] }] },
    });
    expect(decision.allow).toBe(true);
  });

  it('matches persisted Mongo ObjectId user and role identities', () => {
    const userId = new ObjectId();
    const roleId = new ObjectId();
    expect(policy.checkRequestAccess({
      method: 'GET',
      user: { _id: userId, roles: [{ _id: roleId }] },
      routeData: { routePermissions: [{ role: { _id: roleId } }] },
    }).allow).toBe(true);
    expect(policy.checkRequestAccess({
      method: 'GET',
      user: { _id: userId, roles: [] },
      routeData: { routePermissions: [{ allowedUsers: [{ _id: userId }] }] },
    }).allow).toBe(true);
  });

  it('allows a public method config without user', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      routeData: { routeMethodConfig: { isPublic: true } },
    });
    expect(d.allow).toBe(true);
  });

  it('returns 401 for a non-public method config without user', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      routeData: { routeMethodConfig: { isPublic: false } },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(401);
  });

  it('returns 401 when no user and route not published', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      routeData: { routePermissions: [] },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(401);
    expect(d.code).toBe('UNAUTHORIZED');
  });

  it('allows root admin', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { isRootAdmin: true, id: 1 },
      routeData: {},
    });
    expect(d.allow).toBe(true);
  });

  it('returns 403 when route has no permissions', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: {},
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(403);
  });

  it('does not throw when user.roles is empty', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, roles: [] },
      routeData: {
        routePermissions: [{ methods: [{ name: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(false);
  });

  it('does not throw when user.roles is undefined', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1 },
      routeData: {
        routePermissions: [{ methods: [{ name: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(false);
  });

  it.each([
    { isEnabled: false, role: { id: 5 } },
    { isEnabled: false, allowedUsers: [{ id: 1 }] },
  ])('does not grant an explicitly disabled permission %j', permission => {
    const decision = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: { routePermissions: [permission] },
    });
    expect(decision.allow).toBe(false);
  });

  it('matches role by numeric id', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: {
        routePermissions: [{ methods: [{ name: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('matches role by Mongo _id', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { _id: 'abc', roles: [{ _id: 'role1' }] },
      routeData: {
        routePermissions: [
          { methods: [{ name: 'GET' }], role: { _id: 'role1' } },
        ],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('allows a permission granted by any assigned role', () => {
    const d = policy.checkRequestAccess({
      method: 'PATCH',
      user: {
        id: 1,
        roles: [{ id: 'member' }, { id: 'moderator' }],
      },
      routeData: {
        routePermissions: [
          {
            methods: [{ name: 'PATCH' }],
            role: { id: 'moderator' },
          },
        ],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('does not grant a permission for an unassigned role', () => {
    const d = policy.checkRequestAccess({
      method: 'PATCH',
      user: {
        id: 1,
        roles: [{ id: 'member' }, { id: 'moderator' }],
      },
      routeData: {
        routePermissions: [
          {
            methods: [{ name: 'PATCH' }],
            role: { id: 'admin' },
          },
        ],
      },
    });
    expect(d.allow).toBe(false);
  });

  it('matches allowedUsers by _id when role null', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { _id: 'user1', roles: [] },
      routeData: {
        routePermissions: [
          {
            methods: [{ name: 'GET' }],
            allowedUsers: [{ _id: 'user1' }],
            role: { _id: 'role1' },
          },
        ],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('normalizes number vs string role id', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: {
        routePermissions: [{ methods: [{ name: 'GET' }], role: { id: '5' } }],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('treats the resolved method config permissions as the whole decision', () => {
    // route-detect resolves the method config for req.method before policy runs,
    // so every permission row already belongs to the called method.
    const d = policy.checkRequestAccess({
      method: 'DELETE',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: {
        routePermissions: [{ methods: [{ name: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('allows a skipRoleGuard method config for an authenticated user without route permissions', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: {
        routeMethodConfig: { skipRoleGuard: true },
        routePermissions: [],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('allows a skipRoleGuard method config when the user has no role at all', () => {
    const d = policy.checkRequestAccess({
      method: 'PATCH',
      user: { id: 1 },
      routeData: {
        routeMethodConfig: { skipRoleGuard: true },
      },
    });
    expect(d.allow).toBe(true);
  });

  it('still returns 401 for a skipRoleGuard method config without user', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      routeData: {
        routeMethodConfig: { skipRoleGuard: true },
      },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(401);
  });

  it('does not bypass the role check when the method config does not skip the guard', () => {
    const d = policy.checkRequestAccess({
      method: 'DELETE',
      user: { id: 1, roles: [{ id: 5 }] },
      routeData: {
        routeMethodConfig: { skipRoleGuard: false },
        routePermissions: [{ methods: [{ name: 'DELETE' }], role: { id: 9 } }],
      },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(403);
  });
});
