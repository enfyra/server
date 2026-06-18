import { describe, expect, it, vi } from 'vitest';
import { MeService } from '../../src/modules/me/services/me.service';

describe('MeService', () => {
  it('creates a trusted repo context for built-in /me routes without dynamic routeData', async () => {
    const userRepo = {
      find: vi.fn(async () => ({ data: [{ id: 'user-1', email: 'a@test.dev' }] })),
    };
    const context: any = {};
    const dynamicContextFactory = {
      createHttp: vi.fn(() => context),
    };
    const repoRegistryService = {
      createReposProxy: vi.fn(() => ({
        enfyra_user: userRepo,
        secure: {
          enfyra_user: {
            find: vi.fn(async () => {
              throw new Error('secure repo should not be used for /me');
            }),
          },
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
  });
});
