import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import {
  FlowCacheBuilder,
  WebsocketCacheBuilder,
} from '../../src/engines/cache';

describe('script cache builders', () => {
  it('builds flow executable code without persisting script repair during reload', async () => {
    const update = vi.fn(async () => ({}));
    const queryBuilderService = {
      find: vi.fn(async ({ table }: any) => {
        if (table === 'enfyra_flow') {
          return {
            data: [
              {
                id: 1,
                name: 'demo',
                isEnabled: true,
                triggerType: 'manual',
              },
            ],
          };
        }
        if (table === 'enfyra_flow_step') {
          return {
            data: [
              {
                id: 10,
                key: 'script',
                type: 'script',
                isEnabled: true,
                stepOrder: 1,
                scriptLanguage: 'typescript',
                sourceCode: 'const value: string = @BODY.name; return value;',
                compiledCode:
                  'const value: string = $ctx.$body.name; return value;',
                config: {},
              },
            ],
          };
        }
        return { data: [] };
      }),
      update,
    };
    const service = new FlowCacheBuilder({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
    });

    await service.reload(false);

    const flows = await service.getCacheAsync();
    expect(flows[0].steps[0].compiledCode).toContain(
      'const value = $ctx.$body.name;',
    );
    expect(update).not.toHaveBeenCalled();
  });

  it('builds websocket executable code without persisting script repair during reload', async () => {
    const update = vi.fn(async () => ({}));
    const queryBuilderService = {
      isMongoDb: vi.fn(() => false),
      find: vi.fn(async ({ table }: any) => {
        if (table === 'enfyra_websocket') {
          return {
            data: [
              {
                id: 1,
                path: '/chat',
                isEnabled: true,
                sourceCode: 'const value: string = @BODY.name; return value;',
                compiledCode:
                  'const value: string = $ctx.$body.name; return value;',
              },
            ],
          };
        }
        if (table === 'enfyra_websocket_event') {
          return { data: [] };
        }
        return { data: [] };
      }),
      update,
    };
    const service = new WebsocketCacheBuilder({
      queryBuilderService: queryBuilderService as any,
      eventEmitter: new EventEmitter2(),
    });

    await service.reload(false);

    const gateways = await service.getCacheAsync();
    expect(gateways[0].connectionHandlerScript).toContain(
      'const value = $ctx.$body.name;',
    );
    expect(update).not.toHaveBeenCalled();
  });
});
