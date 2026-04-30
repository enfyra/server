import { describe, expect, it, vi } from 'vitest';
import { registerAdminRoutes } from '../../src/http/routes';

function createHarness() {
  const handlers = new Map<string, any>();
  const register = (path: string, handler: any) => handlers.set(path, handler);
  const app = {
    get: register,
    post: register,
    patch: register,
    delete: register,
  };
  const orchestrator = {
    reloadAll: vi.fn().mockResolvedValue(undefined),
    reloadMetadataAndDeps: vi.fn().mockResolvedValue(undefined),
    reloadRoutesOnly: vi.fn().mockResolvedValue(undefined),
    reloadGraphqlOnly: vi.fn().mockResolvedValue(undefined),
    reloadGuardsOnly: vi.fn().mockResolvedValue(undefined),
  };
  registerAdminRoutes(app as any, {
    cradle: { cacheOrchestratorService: orchestrator },
  } as any);

  async function post(path: string) {
    const response = {
      statusCode: 200,
      body: null as any,
      status: vi.fn((code: number) => {
        response.statusCode = code;
        return response;
      }),
      json: vi.fn((body: any) => {
        response.body = body;
        return response;
      }),
    };
    await handlers.get(path)?.({ scope: { cradle: {} } }, response);
    return response;
  }

  return { orchestrator, post };
}

describe('Admin reload endpoints', () => {
  it.each([
    ['/admin/reload', 'reloadAll', 'All cache reload started'],
    [
      '/admin/reload/metadata',
      'reloadMetadataAndDeps',
      'Metadata cache reload started',
    ],
    ['/admin/reload/routes', 'reloadRoutesOnly', 'Route cache reload started'],
    [
      '/admin/reload/graphql',
      'reloadGraphqlOnly',
      'GraphQL cache reload started',
    ],
    ['/admin/reload/guards', 'reloadGuardsOnly', 'Guard cache reload started'],
  ])('%s starts %s without waiting for completion', async (path, method, message) => {
    const { orchestrator, post } = createHarness();
    let resolveReload: (() => void) | undefined;
    orchestrator[method as keyof typeof orchestrator].mockReturnValue(
      new Promise<void>((resolve) => {
        resolveReload = resolve;
      }),
    );

    const response = await post(path);

    expect(response.status).toHaveBeenCalledWith(202);
    expect(response.body).toEqual({
      success: true,
      status: 'accepted',
      message,
    });
    expect(orchestrator[method as keyof typeof orchestrator]).toHaveBeenCalledTimes(1);
    resolveReload?.();
  });

  it('does not fail the response when the background reload rejects', async () => {
    const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { orchestrator, post } = createHarness();
    orchestrator.reloadRoutesOnly.mockRejectedValue(new Error('Redis down'));

    const response = await post('/admin/reload/routes');
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(response.statusCode).toBe(202);
    expect(response.body.status).toBe('accepted');
    expect(consoleSpy).toHaveBeenCalledWith(
      'Error during Route cache reload:',
      expect.any(Error),
    );
    consoleSpy.mockRestore();
  });
});
