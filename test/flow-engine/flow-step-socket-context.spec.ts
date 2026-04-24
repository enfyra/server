import { describe, expect, it } from 'vitest';
import { FlowExecutionQueueService } from '../../src/modules/flow/queues/flow-execution-queue.service';
import { FlowService } from '../../src/modules/flow/services/flow.service';
import { transformCode } from '../../src/domain/shared/code-transformer';
import { WebsocketContextFactory } from '../../src/modules/websocket/services/websocket-context.factory';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';

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

function createDynamicContextFactory(dynamicWebSocketGateway: any) {
  return new DynamicContextFactory({
    bcryptService: {} as any,
    cacheService: {} as any,
    envService: { get: () => 'test-secret' } as any,
    websocketContextFactory: new WebsocketContextFactory({
      dynamicWebSocketGateway,
    }),
  });
}

describe('flow step socket context', () => {
  it('injects $socket into runtime flow script steps', async () => {
    const emitted: Array<{ method: string; args: any[] }> = [];
    const dynamicWebSocketGateway = {
      emitToUser: (...args: any[]) => emitted.push({ method: 'emitToUser', args }),
      emitToRoom: (...args: any[]) => emitted.push({ method: 'emitToRoom', args }),
      emitToNamespace: (...args: any[]) =>
        emitted.push({ method: 'emitToGateway', args }),
      emitToAll: (...args: any[]) => emitted.push({ method: 'broadcast', args }),
      roomSize: async () => 3,
    };
    const dynamicContextFactory = createDynamicContextFactory(
      dynamicWebSocketGateway,
    );

    const service = new FlowExecutionQueueService({
      executorEngineService: new InlineExecutor() as any,
      repoRegistryService: new MockRepoRegistry() as any,
      flowCacheService: {} as any,
      queryBuilderService: { update: async () => undefined } as any,
      websocketEmitService: {} as any,
      dynamicContextFactory,
      flowQueue: {} as any,
    });

    const result = await (service as any).executeFlow(
      {
        id: 1,
        name: 'socket-flow',
        steps: [
          {
            id: 1,
            key: 'notify',
            stepOrder: 1,
            type: 'script',
            config: {
              code: `
                $ctx.$socket.emitToUser(7, 'order:paid', { id: 42 });
                $ctx.$socket.emitToRoom('admins', 'order:paid', { id: 42 });
                return { roomSize: await $ctx.$socket.roomSize('admins') };
              `,
            },
            timeout: 5000,
            onError: 'stop',
            isEnabled: true,
          },
        ],
      },
      { orderId: 42 },
      { id: 7 },
      'exec-1',
      { updateProgress: async () => undefined },
      0,
      [],
    );

    expect(result.context.notify).toEqual({ roomSize: 3 });
    expect(emitted).toEqual([
      { method: 'emitToUser', args: [7, 'order:paid', { id: 42 }] },
      { method: 'emitToRoom', args: ['admins', 'order:paid', { id: 42 }] },
    ]);
  });

  it('captures @SOCKET emits in flow step test mode', async () => {
    const service = new FlowService({
      flowQueue: {} as any,
      flowCacheService: {} as any,
      executorEngineService: {
        run: (code: string, ctx: any) => new InlineExecutor().run(code, ctx),
      } as any,
      repoRegistryService: new MockRepoRegistry() as any,
      dynamicContextFactory: createDynamicContextFactory({}),
    });

    const result = await service.testStep({
      type: 'script',
      config: {
        code: transformCode(`
          @SOCKET.emitToUser(7, 'flow:test', { ok: true });
          return 'sent';
        `),
      },
    });

    expect(result.success).toBe(true);
    expect(result.result).toBe('sent');
    expect(result.emitted).toEqual([
      { method: 'emitToUser', args: [7, 'flow:test', { ok: true }] },
    ]);
  });
});
