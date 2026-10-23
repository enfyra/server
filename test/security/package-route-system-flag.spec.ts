import { describe, expect, it, vi } from 'vitest';
import { registerPackageRoutes } from '../../src/http/routes/package.routes';

function register(body: Record<string, unknown>) {
  const handlers = new Map<string, Function>();
  const app = {
    post: (path: string, handler: Function) => handlers.set(`POST ${path}`, handler),
    patch: (path: string, handler: Function) => handlers.set(`PATCH ${path}`, handler),
    delete: (path: string, handler: Function) => handlers.set(`DELETE ${path}`, handler),
    get: (path: string, handler: Function) => handlers.set(`GET ${path}`, handler),
  };
  const insert = vi.fn(async () => ({ id: 1 }));
  const container = {
    cradle: {
      packageCdnLoaderService: { load: async () => undefined },
      queryBuilderService: { insert, update: async () => undefined },
      dynamicWebSocketGateway: { emitToNamespace: () => undefined },
      eventEmitter: { emit: () => undefined },
    },
  };
  registerPackageRoutes(app as any, container as any);
  const req = {
    body,
    routeData: {
      context: {
        $body: body,
        $user: { id: 'operator' },
        $repos: {
          main: {
            find: async () => ({ data: [] }),
          },
        },
      },
    },
  };
  const res = { json: vi.fn() };
  return { handlers, insert, req, res };
}

describe('package install route system flag', () => {
  it('never persists a caller-supplied isSystem flag', async () => {
    const { handlers, insert, req, res } = register({
      name: 'audit-pkg',
      type: 'App',
      isSystem: true,
    });
    const handler = handlers.get('POST /enfyra_package')!;
    expect(handler).toBeDefined();
    await handler(req, res);
    expect(insert).toHaveBeenCalledWith(
      'enfyra_package',
      expect.objectContaining({ name: 'audit-pkg', isSystem: false }),
    );
  });

  it('keeps the default isSystem false for ordinary installs', async () => {
    const { handlers, insert, req, res } = register({
      name: 'audit-pkg',
      type: 'App',
    });
    const handler = handlers.get('POST /enfyra_package')!;
    await handler(req, res);
    expect(insert).toHaveBeenCalledWith(
      'enfyra_package',
      expect.objectContaining({ isSystem: false }),
    );
  });
});
