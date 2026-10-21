import { describe, expect, it } from 'vitest';
import { FlowService } from '../../src/modules/flow';
import { DynamicContextFactory } from '../../src/shared/services';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';
import { WebsocketContextFactory } from '../../src/modules/websocket';

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

function runtimeRegistryWithFlows(flows: any[] = []) {
  return {
    getActiveData: (identifier: string) =>
      identifier === CACHE_IDENTIFIERS.FLOW ? flows : undefined,
    requireActiveData: (identifier: string) => {
      if (identifier !== CACHE_IDENTIFIERS.FLOW) {
        throw new Error(`unexpected identifier ${identifier}`);
      }
      return flows;
    },
  };
}

function createService() {
  return new FlowService({
    flowQueue: {} as any,
    runtimeRegistryService: runtimeRegistryWithFlows() as any,
    executorEngineService: {
      run: (code: string, ctx: any) => new InlineExecutor().run(code, ctx),
    } as any,
    repoRegistryService: new MockRepoRegistry() as any,
    dynamicContextFactory: new DynamicContextFactory({
      bcryptService: {} as any,
      userCacheService: {} as any,
      envService: { get: () => 'test-secret' } as any,
      websocketContextFactory: new WebsocketContextFactory({
        dynamicWebSocketGateway: {},
      }),
    }),
  });
}

describe('condition step boolean contract', () => {
  it('accepts true and false', async () => {
    const service = createService();

    const truthy = await service.testStep({
      type: 'condition',
      key: 'check',
      sourceCode: 'return true;',
      scriptLanguage: 'javascript',
    });
    expect(truthy.success).toBe(true);
    expect(truthy.result).toBe(true);

    const falsy = await service.testStep({
      type: 'condition',
      key: 'check',
      sourceCode: 'return false;',
      scriptLanguage: 'javascript',
    });
    expect(falsy.success).toBe(true);
    expect(falsy.result).toBe(false);
  });

  it('rejects a condition that returns an object instead of a boolean', async () => {
    const result = await createService().testStep({
      type: 'condition',
      key: 'check',
      sourceCode: 'return { ok: true };',
      scriptLanguage: 'javascript',
    });

    expect(result.success).toBe(false);
    expect(result.error).toContain(
      'Condition step "check" must return a boolean',
    );
    expect(result.error).toContain('got an object');
  });

  it('rejects an empty array and other non-boolean primitives', async () => {
    const service = createService();

    const arrayResult = await service.testStep({
      type: 'condition',
      key: 'check',
      sourceCode: 'return [];',
      scriptLanguage: 'javascript',
    });
    expect(arrayResult.success).toBe(false);
    expect(arrayResult.error).toContain('got an array');

    const numberResult = await service.testStep({
      type: 'condition',
      key: 'check',
      sourceCode: 'return 1;',
      scriptLanguage: 'javascript',
    });
    expect(numberResult.success).toBe(false);
    expect(numberResult.error).toContain('must return a boolean');
  });

  it('rejects a condition with no source code instead of silently branching false', async () => {
    const result = await createService().testStep({
      type: 'condition',
      key: 'empty_check',
      sourceCode: '',
      scriptLanguage: 'javascript',
    });

    expect(result.success).toBe(false);
    expect(result.error).toContain(
      'Condition step "empty_check" needs sourceCode',
    );
  });

  it('keeps script steps free to return any value', async () => {
    const result = await createService().testStep({
      type: 'script',
      key: 'shape',
      sourceCode: 'return { ok: true };',
      scriptLanguage: 'javascript',
    });

    expect(result.success).toBe(true);
    expect(result.result).toEqual({ ok: true });
  });
});
