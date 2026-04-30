import { describe, expect, it } from 'vitest';
import { FlowExecutionQueueService, FlowService } from '../../src/modules/flow';
import { transformCode } from '@enfyra/kernel';
import { WebsocketContextFactory } from '../../src/modules/websocket';
import { DynamicContextFactory } from '../../src/shared/services';

class InlineExecutor {
  async run(code: string, ctx: any) {
    const consoleProxy = {
      log: (...args: any[]) => ctx.$logs(...args),
      warn: (...args: any[]) => ctx.$logs(...args),
      error: (...args: any[]) => ctx.$logs(...args),
      info: (...args: any[]) => ctx.$logs(...args),
    };
    const fn = new Function(
      '$ctx',
      'console',
      `return (async () => { ${code} })()`,
    );
    return fn(ctx, consoleProxy);
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
    userCacheService: {} as any,
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
      emitToUser: (...args: any[]) =>
        emitted.push({ method: 'emitToUser', args }),
      emitToRoom: (...args: any[]) =>
        emitted.push({ method: 'emitToRoom', args }),
      emitToNamespace: (...args: any[]) =>
        emitted.push({ method: 'emitToGateway', args }),
      emitToAll: (...args: any[]) =>
        emitted.push({ method: 'broadcast', args }),
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
      envService: { get: () => 'test' } as any,
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

  it('returns console logs from flow step test mode', async () => {
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
        code: `
          console.log('visible-log', { ok: true });
          return 'done';
        `,
      },
    });

    expect(result.success).toBe(true);
    expect(result.result).toBe('done');
    expect(result.logs).toEqual(['visible-log', { ok: true }]);
  });

  it('primes @FLOW_LAST from previous live flow steps in test mode', async () => {
    const service = new FlowService({
      flowQueue: {} as any,
      flowCacheService: {
        getFlows: async () => [
          {
            id: 1,
            name: 'send-mail',
            triggerType: 'manual',
            isEnabled: true,
            steps: [
              {
                id: 11,
                key: 'get_mail_config',
                stepOrder: 1,
                type: 'condition',
                config: {
                  sourceCode: "return { mailUser: 'sender@test.com' };",
                  scriptLanguage: 'typescript',
                },
                timeout: 5000,
                onError: 'stop',
                retryAttempts: 0,
                isEnabled: true,
              },
              {
                id: 12,
                key: 'send_mail',
                stepOrder: 1,
                type: 'script',
                config: {
                  sourceCode: 'return @FLOW_LAST;',
                  scriptLanguage: 'typescript',
                },
                timeout: 5000,
                onError: 'stop',
                retryAttempts: 0,
                isEnabled: true,
                parentId: 11,
                branch: 'true',
              },
            ],
          },
        ],
      } as any,
      executorEngineService: {
        run: (code: string, ctx: any) => new InlineExecutor().run(code, ctx),
      } as any,
      repoRegistryService: new MockRepoRegistry() as any,
      dynamicContextFactory: createDynamicContextFactory({}),
    });

    const result = await service.testStep({
      id: 12,
      type: 'script',
      key: 'send_mail',
      config: {
        sourceCode: 'return @FLOW_LAST;',
        scriptLanguage: 'typescript',
      },
    });

    expect(result.success).toBe(true);
    expect(result.result).toEqual({ mailUser: 'sender@test.com' });
    expect(result.flowContext?.get_mail_config).toEqual({
      mailUser: 'sender@test.com',
    });
    expect(result.flowContext?.$last).toEqual({ mailUser: 'sender@test.com' });
  });
});
