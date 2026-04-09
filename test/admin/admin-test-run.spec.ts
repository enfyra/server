import { AdminController } from '../../src/modules/admin/admin.controller';

describe('AdminController /admin/test/run', () => {
  function createController(
    overrides?: Partial<{
      flowService: any;
      handlerExecutorService: any;
      repoRegistryService: any;
    }>,
  ) {
    const metadataCacheService = { reload: jest.fn() } as any;
    const routeCacheService = { reload: jest.fn() } as any;
    const graphqlService = { reloadSchema: jest.fn() } as any;
    const flowService = overrides?.flowService ?? { testStep: jest.fn() };
    const handlerExecutorService = overrides?.handlerExecutorService ?? {
      run: jest.fn(),
    };
    const repoRegistryService = overrides?.repoRegistryService ?? {
      createReposProxy: jest.fn(() => ({})),
    };
    const guardCacheService = { reload: jest.fn() } as any;

    return {
      controller: new AdminController(
        metadataCacheService,
        routeCacheService,
        graphqlService,
        flowService,
        handlerExecutorService,
        repoRegistryService,
        guardCacheService,
      ),
      flowService,
      handlerExecutorService,
      repoRegistryService,
    };
  }

  it('runs flow_step through flowService.testStep', async () => {
    const { controller, flowService } = createController();
    flowService.testStep.mockResolvedValue({
      success: true,
      result: { ok: true },
    });

    const res = await controller.runTest({
      kind: 'flow_step',
      type: 'script',
      config: { code: 'return 1' },
      timeout: 1234,
      mockFlow: { $payload: { a: 1 } },
    });

    expect(flowService.testStep).toHaveBeenCalledWith(
      { type: 'script', config: { code: 'return 1' }, timeout: 1234 },
      { $payload: { a: 1 } },
    );
    expect(res).toEqual({ success: true, result: { ok: true } });
  });

  it('returns error for invalid kind', async () => {
    const { controller } = createController();
    const res = await controller.runTest({ kind: 'nope' });
    expect(res).toEqual({
      success: false,
      error: { code: 'INVALID_TEST_KIND', message: 'Invalid test kind' },
    });
  });

  it('runs websocket_event and returns result/logs/emitted', async () => {
    const { controller, handlerExecutorService, repoRegistryService } =
      createController();
    repoRegistryService.createReposProxy.mockReturnValue({ any: 'repo' });

    handlerExecutorService.run.mockImplementation(
      async (_code: string, ctx: any) => {
        ctx.$logs('hello');
        ctx.$socket.emitToGateway('evt', { a: 1 });
        return { ok: true };
      },
    );

    const res = await controller.runTest({
      kind: 'websocket_event',
      gatewayPath: '/chat',
      eventName: 'message',
      timeoutMs: 500,
      payload: { x: 1 },
      script:
        ' @LOGS("hi", @BODY.x); @SOCKET.send("evt", {a:1}); return { ok: true }; ',
    });

    expect(handlerExecutorService.run).toHaveBeenCalled();
    const [transformedCode, ctx, timeoutMs] =
      handlerExecutorService.run.mock.calls[0];
    expect(typeof transformedCode).toBe('string');
    expect(transformedCode).not.toContain('@BODY');
    expect(timeoutMs).toBe(500);

    expect(ctx.$req?.method).toBe('WS_EVENT_TEST');
    expect(ctx.$req?.url).toBe('/chat/message');
    expect(ctx.$socket).toBeDefined();
    expect(ctx.$helpers?.$fetch).toBeDefined();

    expect(res.success).toBe(true);
    expect(res.result).toEqual({ ok: true });
    expect(res.logs).toEqual(['hello']);
    expect(res.emitted).toEqual([
      { method: 'emitToGateway', args: ['evt', { a: 1 }] },
    ]);
  });

  it('websocket_event validates required fields', async () => {
    const { controller } = createController();
    expect(
      await controller.runTest({
        kind: 'websocket_event',
        gatewayPath: '/x',
        script: 'return 1',
      }),
    ).toEqual({
      success: false,
      error: { code: 'MISSING_EVENT_NAME', message: 'eventName is required' },
    });
    expect(
      await controller.runTest({
        kind: 'websocket_event',
        gatewayPath: '/x',
        eventName: 'e',
      }),
    ).toEqual({
      success: false,
      error: { code: 'MISSING_SCRIPT', message: 'script is required' },
    });
  });

  it('runs websocket_connection and returns result/logs/emitted', async () => {
    const { controller, handlerExecutorService, repoRegistryService } =
      createController();
    repoRegistryService.createReposProxy.mockReturnValue({ any: 'repo' });

    handlerExecutorService.run.mockImplementation(
      async (_code: string, ctx: any) => {
        ctx.$logs('connected');
        ctx.$socket.emitToGateway('ws:noti', { ok: true });
        return { ok: true };
      },
    );

    const res = await controller.runTest({
      kind: 'websocket_connection',
      gatewayPath: '/ws',
      timeoutMs: 500,
      payload: { ip: '1.2.3.4' },
      script:
        ' @LOGS("connected"); @SOCKET.send("ws:noti", {ok:true}); return { ok: true }; ',
    });

    expect(handlerExecutorService.run).toHaveBeenCalled();
    const [_transformedCode, ctx, timeoutMs] =
      handlerExecutorService.run.mock.calls[0];
    expect(timeoutMs).toBe(500);
    expect(ctx.$req?.method).toBe('WS_CONNECT_TEST');
    expect(ctx.$req?.url).toBe('/ws');

    expect(res.success).toBe(true);
    expect(res.result).toEqual({ ok: true });
    expect(res.logs).toEqual(['connected']);
    expect(res.emitted).toEqual([
      { method: 'emitToGateway', args: ['ws:noti', { ok: true }] },
    ]);
  });

  it('websocket_connection validates required fields', async () => {
    const { controller } = createController();
    expect(
      await controller.runTest({
        kind: 'websocket_connection',
        gatewayPath: '/x',
      }),
    ).toEqual({
      success: false,
      error: { code: 'MISSING_SCRIPT', message: 'script is required' },
    });
  });

  it('backward-compatible endpoints delegate to runTest', async () => {
    const { controller, flowService, handlerExecutorService } =
      createController();
    flowService.testStep.mockResolvedValue({ success: true });
    handlerExecutorService.run.mockResolvedValue({ ok: true });

    await controller.testFlowStep({
      type: 'script',
      config: {},
      timeout: 1,
      mockFlow: { $payload: {} },
    });
    expect(flowService.testStep).toHaveBeenCalled();

    await controller.testWebsocketEvent({
      gatewayPath: '/x',
      eventName: 'e',
      timeoutMs: 1,
      payload: {},
      script: 'return 1',
    });
    expect(handlerExecutorService.run).toHaveBeenCalled();
  });
});
