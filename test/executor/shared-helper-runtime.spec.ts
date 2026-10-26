import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  ExecutorEngineService,
  IsolatedExecutorService,
  getIoAbortSignal,
} from '@enfyra/kernel';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { DynamicContextFactory } from '../../src/shared/services/dynamic-context.factory';
import { deferDynamicTransactionEffect } from '../../src/shared/utils/dynamic-transaction-effects.util';
import { compileScriptSource } from '../../src/shared/utils/script-code.util';
import type { SharedHelperDefinition } from '../../src/shared/types/shared-helper.types';

const instances: IsolatedExecutorService[] = [];
afterEach(() => {
  for (const instance of instances.splice(0)) instance.onDestroy();
});

function fixture(
  packages: Array<{
    name: string;
    safeName: string;
    version: string;
    sourceCode: string;
  }> = [],
) {
  const isolated = new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => packages.map((pkg) => pkg.name),
    },
    packageCdnLoaderService: { getPackageSources: () => packages },
    tuning: {
      maxConcurrentWorkers: 1,
      isolateMemoryLimitMb: 64,
      isolatesPerWorker: 2,
    },
  });
  instances.push(isolated);
  const helpers: SharedHelperDefinition[] = [
    {
      name: 'echo',
      isEnabled: true,
      code: 'return { params: $ctx.$params, user: $ctx.$user, body: $ctx.$body, query: $ctx.$query };',
    },
    {
      name: 'composed',
      isEnabled: true,
      code: 'return await $ctx.$helpers.run("echo", $ctx.$params);',
    },
    { name: 'disabled', isEnabled: false, code: '' },
    {
      name: 'recurse',
      isEnabled: true,
      code: 'return await $ctx.$helpers.run("recurse", $ctx.$params);',
    },
    {
      name: 'write',
      isEnabled: true,
      code: 'return await $ctx.$repos.secure.items.create($ctx.$params);',
    },
    {
      name: 'fail',
      isEnabled: true,
      code: 'throw new Error("helper-failure");',
    },
  ];
  const service = new RuntimeScriptExecutorService({
    kernelExecutorEngineService: new ExecutorEngineService({
      isolatedExecutorService: isolated,
    }),
    runtimeRegistryService: {
      getSharedHelpers: () => helpers.map((helper) => ({ ...helper })),
    },
  } as any);
  const context: any = {
    $user: { id: 'alice' },
    $body: { value: 'body' },
    $params: { routeId: 1 },
    $query: { caller: true },
    $share: { $logs: [] },
    $helpers: {},
    $repos: {},
    $cache: {},
  };
  const run = (source: string, timeout = 3000, ctx = context) =>
    service.run(compileScriptSource(source, 'typescript')!, ctx, timeout, {
      sourceCode: source,
    });
  return { isolated, service, context, helpers, run };
}

describe('Server-owned same-task helper execution on the installed Kernel', () => {
  it('uses separate params and data without inheriting caller locals', async () => {
    const { run, context, helpers } = fixture();
    helpers.push({
      name: 'isolated',
      isEnabled: true,
      code: 'return { params: $ctx.$params, body: $ctx.$body, query: $ctx.$query, user: $ctx.$user, share: $ctx.$share, data: $ctx.$data, req: $ctx.$req, api: $ctx.$api, inputType: typeof input, callerLocalType: typeof callerLocal, capabilitiesType: typeof capabilities };',
    });
    const result = await run(
      'const callerLocal = "private"; return await @HELPERS.run("isolated", { query: "test" });',
    );
    expect(result).toEqual({
      params: { query: 'test' },
      body: {},
      query: {},
      user: null,
      share: {},
      data: undefined,
      req: undefined,
      api: undefined,
      inputType: 'undefined',
      callerLocalType: 'undefined',
      capabilitiesType: 'undefined',
    });
    expect(context.$body).toEqual({ value: 'body' });
    expect(context.$params).toEqual({ routeId: 1 });
  });

  it('composes helpers and keeps caller data unchanged by helper writes', async () => {
    const { run, helpers, context } = fixture();
    helpers.push({
      name: 'mutate',
      isEnabled: true,
      code: '$ctx.$body.value = "helper"; $ctx.$share.private = true; $ctx.$data = 7; return await $ctx.$helpers.run("composed", { value: 9 });',
    });
    expect(await run('return await @HELPERS.run("mutate");')).toEqual({
      params: { value: 9 },
      user: null,
      body: {},
      query: {},
    });
    expect(context.$body.value).toBe('body');
    expect(context.$share.private).toBeUndefined();
    expect(context.$data).toBeUndefined();
  });

  it.each(['null', 'false', '0', '[1,2]'])(
    'preserves explicit params %s',
    async (value) => {
      const { run } = fixture();
      expect(
        (await run(`return await @HELPERS.run("echo", ${value});`)).params,
      ).toEqual(JSON.parse(value));
    },
  );

  it('uses one helper snapshot and call budget across pre-hook, handler and post-hook', async () => {
    const { service, context } = fixture();
    const { value } = await service.runBatch(
      {
        routeData: {
          context,
          __codeBlocks: [
            {
              type: 'preHook',
              code: '$ctx.$share.pre = await $ctx.$helpers.run("echo", "pre");',
            },
            {
              type: 'handler',
              code: 'return await $ctx.$helpers.run("echo", "handler");',
            },
            {
              type: 'postHook',
              code: '$ctx.$data.post = await $ctx.$helpers.run("echo", "post");',
            },
          ],
        },
      },
      3000,
    );
    expect(value.params).toBe('handler');
    expect(value.post.params).toBe('post');
    expect(context.$share.pre.params).toBe('pre');
    await expect(
      service.runBatch(
        {
          routeData: {
            context,
            __codeBlocks: [
              {
                type: 'preHook',
                code: 'for (let i = 0; i < 600; i++) await $ctx.$helpers.run("echo");',
              },
              {
                type: 'handler',
                code: 'for (let i = 0; i < 425; i++) await $ctx.$helpers.run("echo"); return true;',
              },
            ],
          },
        },
        5000,
      ),
    ).rejects.toThrow(/call limit/i);
  });

  it('supports post-hook-only batches and preserves pre-hook short-circuit behavior', async () => {
    const { service, context } = fixture();
    await service.runBatch(
      {
        routeData: {
          context,
          __codeBlocks: [
            {
              type: 'postHook',
              code: '$ctx.$share.post = await $ctx.$helpers.run("echo", "post-only");',
            },
          ],
        },
      },
      3000,
    );
    expect(context.$share.post.params).toBe('post-only');
    const result = await service.runBatch(
      {
        routeData: {
          context,
          __codeBlocks: [
            {
              type: 'preHook',
              code: 'return await $ctx.$helpers.run("echo", "short");',
            },
            {
              type: 'handler',
              code: 'throw new Error("handler must not run");',
            },
            {
              type: 'postHook',
              code: 'throw new Error("post-hook must not run");',
            },
          ],
        },
      },
      3000,
    );
    expect(result).toMatchObject({
      shortCircuit: true,
      value: { params: 'short' },
    });
    await expect(
      service.runBatch(
        {
          routeData: {
            context,
            __codeBlocks: [
              { type: 'preHook', code: 'await $ctx.$helpers.run("fail");' },
              {
                type: 'postHook',
                code: '$ctx.$share.failurePost = await $ctx.$helpers.run("echo", "failure-post");',
              },
            ],
          },
        },
        3000,
      ),
    ).rejects.toThrow('helper-failure');
    expect(context.$share.failurePost.params).toBe('failure-post');
  });

  it('reports missing helpers when the activated snapshot is empty', async () => {
    const { run, helpers } = fixture();
    helpers.splice(0);
    await expect(run('return await @HELPERS.run("absent");')).rejects.toThrow(
      /Helper not found/i,
    );
  });

  it('reuses a stable activated snapshot and adopts a replacement on the next task', async () => {
    const { isolated, context } = fixture();
    let active: readonly SharedHelperDefinition[] = [
      { name: 'revision', isEnabled: true, code: 'return "old";' },
    ];
    const service = new RuntimeScriptExecutorService({
      kernelExecutorEngineService: new ExecutorEngineService({
        isolatedExecutorService: isolated,
      }),
      runtimeRegistryService: { getSharedHelpers: () => active },
    } as any);
    const code = compileScriptSource(
      'return await @HELPERS.run("revision");',
      'typescript',
    )!;
    expect(await service.run(code, context, 3000)).toBe('old');
    const runtime = (service as any).compiledHelperRuntime;
    expect(await service.run(code, { ...context, $share: {} }, 3000)).toBe(
      'old',
    );
    expect((service as any).compiledHelperRuntime).toBe(runtime);
    active = [{ name: 'revision', isEnabled: true, code: 'return "new";' }];
    expect(await service.run(code, context, 3000)).toBe('new');
    expect((service as any).compiledHelperRuntime).not.toBe(runtime);
  });

  it.each(['missing', 'disabled'])('fails explicitly for %s', async (name) => {
    const { run } = fixture();
    await expect(
      run(`return await @HELPERS.run(${JSON.stringify(name)}, {});`),
    ).rejects.toThrow(name);
  });

  it('bounds recursion without counting parallel siblings as depth', async () => {
    const { run } = fixture();
    await expect(
      run('return await @HELPERS.run("recurse", {});'),
    ).rejects.toThrow(/depth limit/i);
    expect(
      await run(
        'return await Promise.all(Array.from({ length: 40 }, (_, i) => @HELPERS.run("composed", i)));',
      ),
    ).toHaveLength(40);
  });

  it('retains built-in helpers, secure repository authorization and task packages', async () => {
    const { run, context, helpers } = fixture([
      {
        name: 'helper_fixture',
        safeName: 'helper_fixture',
        version: 'fixture',
        sourceCode: 'export default { echo(value) { return value; } };',
      },
    ]);
    context.$helpers.upper = async (value: string) => value.toUpperCase();
    const secureRead = vi.fn(async () => {
      throw new Error('denied for alice');
    });
    context.$repos.secure = { items: { find: secureRead } };
    helpers.push({
      name: 'package_echo',
      isEnabled: true,
      code: 'const pkg = require("helper_fixture"); return await $ctx.$helpers.upper(pkg.echo($ctx.$params));',
    });
    helpers.push({
      name: 'secure_read',
      isEnabled: true,
      code: 'return await $ctx.$repos.secure.items.find({});',
    });
    expect(
      await run('return await @HELPERS.run("package_echo", "value");'),
    ).toBe('VALUE');
    await expect(
      run(
        'return await @HELPERS.run("secure_read", { user: { isRootAdmin: true } });',
      ),
    ).rejects.toThrow('denied for alice');
    expect(secureRead).toHaveBeenCalledOnce();
  });

  it('isolates mutable helper locals and params across concurrent tasks', async () => {
    const { run, helpers, context } = fixture();
    helpers.push({
      name: 'local',
      isEnabled: true,
      code: 'let calls = 0; calls++; return { calls, user: $ctx.$user, params: $ctx.$params };',
    });
    const results = await Promise.all([
      run('return await @HELPERS.run("local", "alice");'),
      run('return await @HELPERS.run("local", "bob");', 3000, {
        ...context,
        $user: { id: 'bob' },
        $share: {},
      }),
    ]);
    expect(results).toEqual([
      { calls: 1, user: null, params: 'alice' },
      { calls: 1, user: null, params: 'bob' },
    ]);
  });

  it('names helper failures and preserves structured 403 throws', async () => {
    const { run, helpers } = fixture();
    await expect(run('return await @HELPERS.run("fail");')).rejects.toThrow(
      /helper.*fail.*helper-failure/i,
    );
    helpers.push({
      name: 'deny',
      isEnabled: true,
      code: '$ctx.$throw.http(403, "denied");',
    });
    await expect(
      run('return await @HELPERS.run("deny");'),
    ).rejects.toMatchObject({ statusCode: 403 });
  });

  it('preserves caller source locations and repairs stale code with an active helper snapshot', async () => {
    const { run, service, context, helpers } = fixture();
    await expect(
      run('const first = 1;\nconst second = 2;\nreturn unknownCaller;'),
    ).rejects.toMatchObject({ details: { phase: 'handler', line: 3 } });
    let repaired = '';
    expect(
      await service.run(
        'const value: string = $ctx.$body.value; return value;',
        context,
        3000,
        {
          sourceCode:
            'const value: string = @BODY.value; return await @HELPERS.run("echo", value);',
          onCompiledCodeRepair: (code) => {
            repaired = code;
            helpers[0].code = 'return "later snapshot";';
          },
        },
      ),
    ).toMatchObject({ params: 'body', user: null });
    expect(repaired).not.toContain(': string');
    expect(await run('return await @HELPERS.run("echo");')).toBe(
      'later snapshot',
    );
  });

  it('retains native CPU timeout and recovers task capacity', async () => {
    const { run, helpers } = fixture();
    helpers.push({ name: 'spin', isEnabled: true, code: 'while (true) {}' });
    await expect(
      run('return await @HELPERS.run("spin");', 150),
    ).rejects.toThrow(/timed out/i);
    expect(await run('return 42;')).toBe(42);
  });

  it('cancels helper host I/O without cancelling a sibling', async () => {
    const { service, context, helpers, run } = fixture();
    helpers.push({
      name: 'wait',
      isEnabled: true,
      code: 'return await $ctx.$helpers.wait();',
    });
    const controller = new AbortController();
    let entered = false;
    let aborted = false;
    context.$helpers.wait = async () => {
      entered = true;
      await new Promise<void>((resolve) => {
        const signal = getIoAbortSignal();
        if (signal?.aborted) {
          aborted = true;
          resolve();
        } else
          signal?.addEventListener(
            'abort',
            () => {
              aborted = true;
              resolve();
            },
            { once: true },
          );
      });
    };
    const pending = service
      .runBatch(
        {
          routeData: {
            context,
            __codeBlocks: [
              {
                type: 'handler',
                code: 'return await $ctx.$helpers.run("wait");',
              },
            ],
          },
        },
        3000,
        { signal: controller.signal },
      )
      .catch((error) => error);
    await vi.waitFor(() => expect(entered).toBe(true), { timeout: 2000 });
    const sibling = run('return 7;', 3000, { ...context, $share: {} });
    controller.abort();
    expect((await pending).code).toBe('ERR_EXECUTION_ABORTED');
    expect(aborted).toBe(true);
    expect(await sibling).toBe(7);
  });

  it.each(['none', 'caller', 'helper'])(
    'shares uncommitted reads and outer commit/rollback when failure is %s',
    async (failure) => {
      const { run, helpers } = fixture();
      const events: string[] = [];
      let persisted: string[] = [];
      let pending: string[] | undefined;
      const factory = new DynamicContextFactory({
        userCacheService: {},
        envService: { get: () => 'test' },
        databaseConfigService: { isMongoDb: () => false },
        knexService: {
          transaction: async (work: (trx: object) => Promise<unknown>) => {
            events.push('begin');
            pending = [...persisted];
            try {
              const result = await work({});
              persisted = pending;
              events.push('commit');
              return result;
            } catch (error) {
              events.push('rollback');
              throw error;
            } finally {
              pending = undefined;
            }
          },
          runWithTransaction: (_trx: object, work: () => Promise<unknown>) =>
            work(),
        },
      } as any);
      const ctx = factory.createBase();
      ctx.$repos = {
        items: {
          create: async (value: string) => {
            (pending ?? persisted).push(value);
            const effect = () => {
              events.push(`published:${value}`);
            };
            if (!deferDynamicTransactionEffect(ctx, effect)) effect();
            return value;
          },
          find: async () => [...(pending ?? persisted)],
        },
      } as any;
      helpers.push({
        name: 'transaction_write',
        isEnabled: true,
        code: 'return await $ctx.$transaction.run(async () => { const before = await $ctx.$repos.items.find(); await $ctx.$repos.items.create("helper"); if ($ctx.$params.fail) throw new Error("helper rollback"); return before; });',
      });
      const source = `return await @TRANSACTION.run(async () => { await #items.create("caller"); const before = await @HELPERS.run("transaction_write", { fail: ${failure === 'helper'} }); if (${failure === 'caller'}) throw new Error("caller rollback"); return { before, after: await #items.find() }; });`;
      if (failure === 'none') {
        expect(await run(source, 5000, ctx)).toEqual({
          before: ['caller'],
          after: ['caller', 'helper'],
        });
        expect(persisted).toEqual(['caller', 'helper']);
        expect(events).toEqual([
          'begin',
          'commit',
          'published:caller',
          'published:helper',
        ]);
      } else {
        await expect(run(source, 5000, ctx)).rejects.toThrow('rollback');
        expect(persisted).toEqual([]);
        expect(events).toEqual(['begin', 'rollback']);
      }
    },
  );
});
