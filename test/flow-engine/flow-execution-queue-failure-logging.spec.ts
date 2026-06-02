import { describe, expect, it, vi } from 'vitest';
import { FlowExecutionQueueService } from '../../src/modules/flow';
import { WebsocketContextFactory } from '../../src/modules/websocket';
import { DynamicContextFactory } from '../../src/shared/services';

class InlineExecutor {
  async run(code: string, ctx: any) {
    const fn = new Function('$ctx', `return (async () => { ${code} })()`);
    return fn(ctx);
  }
}

class MockRepoRegistry {
  createReposProxy() {
    return {};
  }
}

function createDynamicContextFactory() {
  return new DynamicContextFactory({
    bcryptService: {} as any,
    userCacheService: {} as any,
    envService: { get: () => 'test-secret' } as any,
    websocketContextFactory: new WebsocketContextFactory({
      dynamicWebSocketGateway: {},
    }),
  });
}

describe('FlowExecutionQueueService failure diagnostics', () => {
  it('persists running flow progress before completing the same execution history row', async () => {
    const inserts: any[] = [];
    const updates: any[] = [];
    const progressUpdates: any[] = [];
    const flow = {
      id: 13,
      name: 'progress-flow',
      steps: [
        {
          id: 1,
          key: 'prepare',
          stepOrder: 1,
          type: 'script',
          config: { code: "return 'ok';" },
          timeout: 5000,
          onError: 'stop',
          isEnabled: true,
        },
      ],
    };
    const service = new FlowExecutionQueueService({
      executorEngineService: new InlineExecutor() as any,
      repoRegistryService: new MockRepoRegistry() as any,
      flowCacheService: {
        getFlowByName: vi.fn().mockResolvedValue(flow),
        getFlowById: vi.fn().mockResolvedValue(flow),
        reload: vi.fn(),
      } as any,
      queryBuilderService: {
        insert: vi.fn(async (_table: string, data: any) => {
          inserts.push(data);
          return { data: [{ id: 101, ...data }] };
        }),
        update: vi.fn(async (_table: string, id: any, data: any) => {
          updates.push({ id, data });
          return { data: [{ id, ...data }] };
        }),
        find: vi.fn(async () => ({ data: [], meta: { totalCount: 0 } })),
      } as any,
      websocketEmitService: { emitToUser: vi.fn() } as any,
      dynamicContextFactory: createDynamicContextFactory(),
      envService: { get: vi.fn(() => undefined) } as any,
      isolatedExecutorService: {
        getMetrics: () => ({ tuning: { maxConcurrentWorkers: 1 } }),
      } as any,
      flowQueue: {} as any,
    });

    const result = await service.process({
      id: 'job-2',
      data: {
        flowName: 'progress-flow',
        payload: { projectId: 18 },
        triggeredBy: { id: 'user-1' },
      },
      updateProgress: async (progress: any) => {
        progressUpdates.push(progress);
      },
    } as any);

    expect(result.success).toBe(true);
    expect(inserts).toHaveLength(1);
    expect(inserts[0]).toMatchObject({
      flow: 13,
      status: 'running',
      currentStep: null,
      completedSteps: [],
    });
    expect(updates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 101,
          data: expect.objectContaining({
            status: 'running',
            currentStep: 'prepare',
          }),
        }),
        expect.objectContaining({
          id: 101,
          data: expect.objectContaining({
            status: 'completed',
            currentStep: 'prepare',
            completedAt: expect.any(Date),
          }),
        }),
      ]),
    );
    expect(progressUpdates.at(-1)).toMatchObject({
      currentStep: 'prepare',
    });
  });

  it('persists failed flow current step and completed steps in execution history', async () => {
    const inserts: any[] = [];
    const updates: any[] = [];
    const progressUpdates: any[] = [];
    const flow = {
      id: 12,
      name: 'diagnostic-flow',
      steps: [
        {
          id: 1,
          key: 'prepare',
          stepOrder: 1,
          type: 'script',
          config: { code: "return 'ok';" },
          timeout: 5000,
          onError: 'stop',
          isEnabled: true,
        },
        {
          id: 2,
          key: 'deploy',
          stepOrder: 2,
          type: 'script',
          config: { code: "throw new Error('deploy failed');" },
          timeout: 5000,
          onError: 'stop',
          isEnabled: true,
        },
      ],
    };
    const service = new FlowExecutionQueueService({
      executorEngineService: new InlineExecutor() as any,
      repoRegistryService: new MockRepoRegistry() as any,
      flowCacheService: {
        getFlowByName: vi.fn().mockResolvedValue(flow),
        getFlowById: vi.fn().mockResolvedValue(flow),
        reload: vi.fn(),
      } as any,
      queryBuilderService: {
        insert: vi.fn(async (_table: string, data: any) => {
          inserts.push(data);
          return { data: [{ id: 102, ...data }] };
        }),
        update: vi.fn(async (_table: string, id: any, data: any) => {
          updates.push({ id, data });
          return { data: [{ id, ...data }] };
        }),
        find: vi.fn(async () => ({ data: [], meta: { totalCount: 0 } })),
      } as any,
      websocketEmitService: { emitToUser: vi.fn() } as any,
      dynamicContextFactory: createDynamicContextFactory(),
      envService: { get: vi.fn(() => undefined) } as any,
      isolatedExecutorService: {
        getMetrics: () => ({ tuning: { maxConcurrentWorkers: 1 } }),
      } as any,
      flowQueue: {} as any,
    });

    const result = await service.process({
      id: 'job-1',
      data: {
        flowName: 'diagnostic-flow',
        payload: { projectId: 18 },
        triggeredBy: { id: 'user-1' },
      },
      updateProgress: async (progress: any) => {
        progressUpdates.push(progress);
      },
    } as any);
    await Promise.resolve();

    expect(result.success).toBe(false);
    expect(progressUpdates.at(-1)).toMatchObject({
      currentStep: 'deploy',
      failedStep: 'deploy',
    });
    expect(inserts).toHaveLength(1);
    expect(inserts[0]).toMatchObject({
      flow: 12,
      status: 'running',
      currentStep: null,
      completedSteps: [],
    });
    expect(updates).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: 102,
          data: expect.objectContaining({
            status: 'running',
            currentStep: 'prepare',
          }),
        }),
        expect.objectContaining({
          id: 102,
          data: expect.objectContaining({
            status: 'running',
            currentStep: 'deploy',
          }),
        }),
        expect.objectContaining({
          id: 102,
          data: expect.objectContaining({
            status: 'failed',
            currentStep: 'deploy',
            completedAt: expect.any(Date),
            error: expect.objectContaining({
              message: 'deploy failed',
              currentStep: 'deploy',
              failedStep: 'deploy',
            }),
          }),
        }),
      ]),
    );
    const finalUpdate = updates.at(-1);
    expect(finalUpdate.data.completedSteps).toEqual([
      expect.objectContaining({ key: 'prepare' }),
    ]);
  });
});
