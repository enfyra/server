import { describe, expect, it, vi } from 'vitest';
import { MeService } from '../../src/modules/me/services/me.service';

describe('MeService', () => {
  it('creates an enforced repo context for built-in /me reads without dynamic routeData', async () => {
    const userRepo = {
      find: vi.fn(async () => ({
        data: [{ id: 'user-1', email: 'a@test.dev' }],
      })),
    };
    const trustedUserRepo = {
      find: vi.fn(async () => ({
        data: [
          {
            id: 'user-1',
            email: 'a@test.dev',
            password: 'hashed-password',
          },
        ],
      })),
    };
    const context: any = {};
    const dynamicContextFactory = {
      createHttp: vi.fn(() => context),
    };
    const repoRegistryService = {
      createReposProxy: vi.fn(() => ({
        enfyra_user: trustedUserRepo,
        secure: {
          enfyra_user: userRepo,
        },
      })),
    };
    const service = new MeService({
      repoRegistryService: repoRegistryService as any,
      dynamicContextFactory: dynamicContextFactory as any,
      policyService: {} as any,
      runtimeRegistryService: {} as any,
    });

    const result = await service.find({
      user: { id: 'user-1', loginProvider: 'google' },
      method: 'GET',
      url: '/me',
      originalUrl: '/me',
      path: '/me',
      query: {},
      params: {},
      headers: {},
      hostname: 'example.test',
      protocol: 'https',
      ip: '127.0.0.1',
    } as any);

    expect(dynamicContextFactory.createHttp).toHaveBeenCalledOnce();
    expect(repoRegistryService.createReposProxy).toHaveBeenCalledWith(context);
    expect(userRepo.find).toHaveBeenCalledWith({
      filter: { id: { _eq: 'user-1' } },
      limit: 1,
      deep: {
        role: {
          deep: {
            routePermissions: { limit: 0 },
          },
        },
      },
    });
    expect(result.data[0]).toMatchObject({
      id: 'user-1',
      email: 'a@test.dev',
      loginProvider: 'google',
    });
    expect(result.data[0]).not.toHaveProperty('password');
    expect(trustedUserRepo.find).not.toHaveBeenCalled();
  });


  it('strips user relations from self updates without direct user PATCH access', async () => {
    const userRepo = {
      update: vi.fn(async ({ data }) => ({ data: [{ id: 'user-1', ...data }] })),
    };
    const context: any = {};
    const policyService = {
      checkRequestAccess: vi.fn(() => ({ allow: false })),
    };
    const service = new MeService({
      dynamicContextFactory: {
        createHttp: vi.fn(() => context),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn(() => ({
          secure: { enfyra_user: userRepo },
        })),
      } as any,
      policyService: policyService as any,
      runtimeRegistryService: {
        getRoutes: vi.fn(() => [{ path: '/enfyra_user', routePermissions: [] }]),
        requireTableMetadata: vi.fn(() => ({
          relations: [
            { propertyName: 'role' },
            { propertyName: 'allowedRoutePermissions' },
          ],
        })),
      } as any,
    });
    const req = {
      user: { id: 'user-1' },
      method: 'PATCH',
      url: '/me',
      originalUrl: '/me',
      path: '/me',
      query: {},
      params: {},
      headers: {},
      hostname: 'example.test',
      protocol: 'https',
      ip: '127.0.0.1',
    } as any;

    await expect(
      service.update({
        id: 'user-1',
        fullName: 'Safe Profile',
        role: { id: 1 },
        allowedRoutePermissions: [{ id: 2 }],
      }, req),
    ).resolves.toEqual({
      data: [{ id: 'user-1', fullName: 'Safe Profile' }],
    });
    expect(userRepo.update).toHaveBeenCalledTimes(1);
    expect(userRepo.update).toHaveBeenCalledWith({
      id: 'user-1',
      data: { id: 'user-1', fullName: 'Safe Profile' },
    });
  });

  it('keeps user relations when the authenticated user can PATCH users', async () => {
    const userRepo = {
      update: vi.fn(async ({ data }) => ({ data: [{ id: 'user-1', ...data }] })),
    };
    const service = new MeService({
      dynamicContextFactory: {
        createHttp: vi.fn(() => ({})),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn(() => ({
          secure: { enfyra_user: userRepo },
        })),
      } as any,
      policyService: {
        checkRequestAccess: vi.fn(() => ({ allow: true })),
      } as any,
      runtimeRegistryService: {
        getRoutes: vi.fn(() => [{ path: '/enfyra_user', routePermissions: [] }]),
        requireTableMetadata: vi.fn(),
      } as any,
    });
    const req = { user: { id: 'user-1' } } as any;

    await service.update({ fullName: 'Admin', role: { id: 1 } }, req);

    expect(userRepo.update).toHaveBeenCalledWith({
      id: 'user-1',
      data: { fullName: 'Admin', role: { id: 1 } },
    });
  });

  it('keeps user relations for a root administrator without route permission lookup', async () => {
    const userRepo = {
      update: vi.fn(async ({ data }) => ({ data: [{ id: 'root-1', ...data }] })),
    };
    const policyService = {
      checkRequestAccess: vi.fn(),
    };
    const service = new MeService({
      dynamicContextFactory: {
        createHttp: vi.fn(() => ({})),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn(() => ({
          secure: { enfyra_user: userRepo },
        })),
      } as any,
      policyService: policyService as any,
      runtimeRegistryService: {
        getRoutes: vi.fn(),
        requireTableMetadata: vi.fn(),
      } as any,
    });

    await service.update(
      { fullName: 'Root', role: { id: 1 } },
      { user: { id: 'root-1', isRootAdmin: true } } as any,
    );

    expect(userRepo.update).toHaveBeenCalledWith({
      id: 'root-1',
      data: { fullName: 'Root', role: { id: 1 } },
    });
    expect(policyService.checkRequestAccess).not.toHaveBeenCalled();
  });

  it('uses enforced repository reads for /me/oauth-accounts', async () => {
    const oauthRepo = {
      find: vi.fn(async () => ({
        data: [{ id: 'oauth-1', provider: 'google' }],
      })),
    };
    const trustedOauthRepo = {
      find: vi.fn(async () => ({
        data: [
          {
            id: 'oauth-1',
            provider: 'google',
            accessToken: 'secret-access-token',
          },
        ],
      })),
    };
    const context: any = {};
    const service = new MeService({
      dynamicContextFactory: {
        createHttp: vi.fn(() => context),
      } as any,
      repoRegistryService: {
        createReposProxy: vi.fn(() => ({
          enfyra_oauth_account: trustedOauthRepo,
          secure: { enfyra_oauth_account: oauthRepo },
        })),
      } as any,
    });

    const result = await service.findOAuthAccounts({
      user: { id: 'user-1' },
      method: 'GET',
      url: '/me/oauth-accounts',
      originalUrl: '/me/oauth-accounts',
      path: '/me/oauth-accounts',
      query: {},
      params: {},
      headers: {},
      hostname: 'example.test',
      protocol: 'https',
      ip: '127.0.0.1',
    } as any);

    expect(oauthRepo.find).toHaveBeenCalledWith({
      filter: { user: { id: { _eq: 'user-1' } } },
    });
    expect(trustedOauthRepo.find).not.toHaveBeenCalled();
    expect(result.data[0]).not.toHaveProperty('accessToken');
  });

});
