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
    });
    expect(result.data[0]).toMatchObject({
      id: 'user-1',
      email: 'a@test.dev',
      loginProvider: 'google',
    });
    expect(result.data[0]).not.toHaveProperty('password');
    expect(trustedUserRepo.find).not.toHaveBeenCalled();
  });


  it('rejects protected user fields on /me updates while allowing profile fields', async () => {
    const userRepo = {
      update: vi.fn(async ({ data }) => ({ data: [{ id: 'user-1', ...data }] })),
    };
    const tableRepo = {
      find: vi.fn(async () => ({
        data: [
          {
            id: 1,
            name: 'enfyra_user',
            columns: [
              { name: 'id', isPrimary: true, isSystem: true },
              { name: 'email', isSystem: true, isPublished: true, isUpdatable: true },
              { name: 'password', isSystem: true, isPublished: false, isUpdatable: true },
              { name: 'isRootAdmin', isSystem: true, isPublished: true, isUpdatable: false },
              { name: 'isSystem', isSystem: true, isPublished: true, isUpdatable: true },
              { name: 'emailVerificationStatus', isSystem: true, isPublished: true, isUpdatable: true },
              { name: 'fullName', isSystem: false, isPublished: true, isUpdatable: true },
              { name: 'secretNote', isSystem: false, isPublished: false, isUpdatable: true },
            ],
            relations: [{ propertyName: 'role', isSystem: true }],
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
          enfyra_table: tableRepo,
          secure: { enfyra_user: userRepo },
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

    await expect(service.update({ role: { id: 1 } }, req)).rejects.toThrow(
      'Protected user fields cannot be updated through /me: role',
    );
    await expect(service.update({ isSystem: true }, req)).rejects.toThrow(
      'Protected user fields cannot be updated through /me: isSystem',
    );
    await expect(service.update({ email: 'new@test.dev' }, req)).rejects.toThrow(
      'Protected user fields cannot be updated through /me: email',
    );
    await expect(service.update({ secretNote: 'x' }, req)).rejects.toThrow(
      'Protected user fields cannot be updated through /me: secretNote',
    );

    await expect(
      service.update({ fullName: 'Safe Profile', password: 'hashed' }, req),
    ).resolves.toEqual({
      data: [{ id: 'user-1', fullName: 'Safe Profile', password: 'hashed' }],
    });
    expect(userRepo.update).toHaveBeenCalledTimes(1);
    expect(userRepo.update).toHaveBeenCalledWith({
      id: 'user-1',
      data: { fullName: 'Safe Profile', password: 'hashed' },
    });
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
