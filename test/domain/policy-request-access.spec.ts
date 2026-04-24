import { PolicyService } from '../../src/domain/policy/policy.service';

describe('PolicyService.checkRequestAccess', () => {
  const policy = new PolicyService({} as any, {} as any);

  it('allows published methods without user', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      routeData: { publishedMethods: [{ method: 'GET' }] },
    });
    expect(d.allow).toBe(true);
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
      user: { id: 1, role: { id: 5 } },
      routeData: {},
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(403);
  });

  it('does not throw when user.role is null', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, role: null },
      routeData: {
        routePermissions: [{ methods: [{ method: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(false);
  });

  it('does not throw when user.role is undefined', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1 },
      routeData: {
        routePermissions: [{ methods: [{ method: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(false);
  });

  it('matches role by numeric id', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, role: { id: 5 } },
      routeData: {
        routePermissions: [{ methods: [{ method: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('matches role by Mongo _id', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { _id: 'abc', role: { _id: 'role1' } },
      routeData: {
        routePermissions: [
          { methods: [{ method: 'GET' }], role: { _id: 'role1' } },
        ],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('matches allowedUsers by _id when role null', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { _id: 'user1', role: null },
      routeData: {
        routePermissions: [
          {
            methods: [{ method: 'GET' }],
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
      user: { id: 1, role: { id: 5 } },
      routeData: {
        routePermissions: [{ methods: [{ method: 'GET' }], role: { id: '5' } }],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('rejects when method does not match', () => {
    const d = policy.checkRequestAccess({
      method: 'DELETE',
      user: { id: 1, role: { id: 5 } },
      routeData: {
        routePermissions: [{ methods: [{ method: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(403);
  });

  it('allows skipRoleGuardMethods when user authenticated but no routePermissions match', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      user: { id: 1, role: { id: 5 } },
      routeData: {
        skipRoleGuardMethods: [{ method: 'GET' }],
        routePermissions: [],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('allows skipRoleGuardMethods when user has no role at all', () => {
    const d = policy.checkRequestAccess({
      method: 'PATCH',
      user: { id: 1 },
      routeData: {
        skipRoleGuardMethods: [{ method: 'PATCH' }],
      },
    });
    expect(d.allow).toBe(true);
  });

  it('still returns 401 when route is in skipRoleGuardMethods but no user', () => {
    const d = policy.checkRequestAccess({
      method: 'GET',
      routeData: {
        skipRoleGuardMethods: [{ method: 'GET' }],
      },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(401);
  });

  it('does not bypass role check when method is not in skipRoleGuardMethods', () => {
    const d = policy.checkRequestAccess({
      method: 'DELETE',
      user: { id: 1, role: { id: 5 } },
      routeData: {
        skipRoleGuardMethods: [{ method: 'GET' }],
        routePermissions: [{ methods: [{ method: 'GET' }], role: { id: 5 } }],
      },
    });
    expect(d.allow).toBe(false);
    expect(d.statusCode).toBe(403);
  });
});
