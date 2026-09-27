import { EventEmitter2 } from 'eventemitter2';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';
import { FlowCacheBuilder } from '../../src/engines/cache/services/flow-cache-builder.service';
import * as scriptCode from '../../src/shared/utils/script-code.util';
import { DatabaseConfigService } from '../../src/shared/services';

function createRoutes() {
  return [{ id: 2, path: '/probe', preHooks: [], postHooks: [] }];
}

describe('cold-start cache dependencies', () => {
  beforeEach(() => DatabaseConfigService.overrideForTesting('postgres'));

  it('hydrates global hooks only after the method map is ready', async () => {
    let releaseMethods!: () => void;
    const methodsReady = new Promise<void>((resolve) => {
      releaseMethods = resolve;
    });
    const calls: string[] = [];
    const builder = new RouteCacheService({
      eventEmitter: new EventEmitter2(),
      metadataCacheService: {
        getMetadata: async () => ({ tables: [] }),
      } as any,
      queryBuilderService: {
        isMongoDb: () => false,
        find: async ({ table }: { table: string }) => {
          calls.push(table);
          if (table === 'enfyra_method') {
            await methodsReady;
            return { data: [{ id: 1, name: 'GET' }] };
          }
          return {
            data:
              table === 'enfyra_route'
                ? createRoutes()
                : [
                    {
                      id: 7,
                      isGlobal: true,
                      isEnabled: true,
                      methods: [{ id: 1 }],
                    },
                  ],
          };
        },
      } as any,
    });
    const loading = (builder as any).loadFromDb();
    await Promise.resolve();
    expect(calls).toContain('enfyra_route');
    releaseMethods();
    const result = await loading;
    expect(result.routes[0].preHooks[0].methods).toEqual([
      { id: 1, name: 'GET' },
    ]);
    expect(result.routes[0].postHooks[0].methods).toEqual([
      { id: 1, name: 'GET' },
    ]);
  });

  it.each(['postgres', 'mysql', 'mongodb'] as const)(
    'loads all pages of enabled flow children on %s',
    async (backend) => {
      DatabaseConfigService.overrideForTesting(backend);
      const pk = DatabaseConfigService.getPkField();
      const flows = Array.from({ length: 60 }, (_, i) => ({
        [pk]: i + 1,
        isEnabled: true,
      }));
      const steps = flows.flatMap((flow) =>
        Array.from({ length: 201 }, (_, i) => ({
          [pk]: flow[pk] * 1000 + i,
          flow: { [pk]: flow[pk] },
          type: 'delay',
          stepOrder: i,
          isEnabled: true,
        })),
      );
      const triggers = flows.flatMap((flow) =>
        Array.from({ length: 90 }, (_, i) => ({
          [pk]: flow[pk] * 100 + i,
          flow: { [pk]: flow[pk] },
          type: 'manual',
          isEnabled: true,
        })),
      );
      const find = jest.fn(async ({ table, limit = 100, page = 1 }: any) => {
        const rows =
          table === 'enfyra_flow'
            ? flows
            : table === 'enfyra_flow_step'
              ? steps
              : triggers;
        return { data: rows.slice((page - 1) * limit, page * limit) };
      });
      const builder = new FlowCacheBuilder({
        eventEmitter: new EventEmitter2(),
        queryBuilderService: { find } as any,
      });
      const result = await (builder as any).loadFromDb();
      expect(result).toHaveLength(flows.length);
      expect(
        result.reduce((n: number, flow: any) => n + flow.steps.length, 0),
      ).toBe(steps.length);
      expect(
        result.reduce((n: number, flow: any) => n + flow.triggers.length, 0),
      ).toBe(triggers.length);
      for (const call of find.mock.calls) {
        expect(call[0].sort).toEqual([pk]);
      }
    },
  );

  it('compiles each global hook once, not once per route', async () => {
    const normalize = jest.spyOn(scriptCode, 'normalizeScriptRecord');
    try {
      const builder = new RouteCacheService({
        eventEmitter: new EventEmitter2(),
        metadataCacheService: {
          getMetadata: async () => ({ tables: [] }),
        } as any,
        queryBuilderService: {
          isMongoDb: () => false,
          find: async ({ table }: any) => ({
            data:
              table === 'enfyra_method'
                ? [{ id: 1, name: 'GET' }]
                : table === 'enfyra_route'
                  ? Array.from({ length: 40 }, (_, i) => ({
                      id: i + 1,
                      path: `/r${i}`,
                      preHooks: [],
                      postHooks: [],
                    }))
                  : table === 'enfyra_pre_hook'
                    ? [
                        {
                          id: 7,
                          isGlobal: true,
                          isEnabled: true,
                          methods: [{ id: 1 }],
                          sourceCode: 'const name: string = "ok";',
                          compiledCode: 'stale',
                        },
                      ]
                    : [],
          }),
        } as any,
      });
      const result = await (builder as any).loadFromDb();
      expect(normalize).toHaveBeenCalledTimes(1);
      expect(
        result.routes.every((route: any) =>
          route.preHooks[0].code.includes('const name ='),
        ),
      ).toBe(true);
    } finally {
      normalize.mockRestore();
    }
  });

  it('uses freshly normalized flow code without compiling it twice', async () => {
    const resolve = jest.spyOn(scriptCode, 'resolveExecutableScript');
    const normalize = jest.spyOn(scriptCode, 'normalizeFlowStepScriptConfig');
    try {
      const builder = new FlowCacheBuilder({
        eventEmitter: new EventEmitter2(),
        queryBuilderService: {
          find: async ({ table }: any) => ({
            data:
              table === 'enfyra_flow'
                ? [{ id: 1, isEnabled: true }]
                : table === 'enfyra_flow_step'
                  ? [
                      {
                        id: 2,
                        flowId: 1,
                        type: 'script',
                        isEnabled: true,
                        sourceCode: 'const value: string = "ok";',
                        compiledCode: 'stale',
                        config: {},
                      },
                    ]
                  : [],
          }),
        } as any,
      });
      const result = await (builder as any).loadFromDb();
      expect(normalize).toHaveBeenCalledTimes(1);
      expect(resolve).not.toHaveBeenCalled();
      expect(result[0].steps[0].compiledCode).toContain('const value =');
      expect(result[0].steps[0].config.code).toBe(
        result[0].steps[0].compiledCode,
      );
    } finally {
      resolve.mockRestore();
      normalize.mockRestore();
    }
  });

  it('paginates the parent flow list without a total-record cap', async () => {
    const flows = Array.from({ length: 1001 }, (_, index) => ({
      id: index + 1,
      isEnabled: true,
    }));
    const find = jest.fn(async ({ table, page = 1, limit }: any) => ({
      data:
        table === 'enfyra_flow'
          ? flows.slice((page - 1) * limit, page * limit)
          : [],
    }));
    const builder = new FlowCacheBuilder({
      eventEmitter: new EventEmitter2(),
      queryBuilderService: { find } as any,
    });
    expect(await (builder as any).loadFromDb()).toHaveLength(1001);
    expect(
      find.mock.calls.filter((call) => call[0].table === 'enfyra_flow'),
    ).toHaveLength(2);
  });
});
