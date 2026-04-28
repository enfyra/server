import { EventEmitter2 } from 'eventemitter2';
import { RouteCacheService } from 'src/engines/cache';

describe('script cache repair', () => {
  it('persists repaired compiledCode only when cached compiledCode is invalid', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const service = new RouteCacheService({
      queryBuilderService: { update } as any,
      metadataCacheService: {} as any,
      eventEmitter: new EventEmitter2(),
    });

    const code = await (service as any).resolveAndRepairScript(
      'route_handler_definition',
      {
        id: 10,
        scriptLanguage: 'typescript',
        sourceCode: 'const value: string = @BODY.name; return value;',
        compiledCode: 'const value: string = $ctx.$body.name; return value;',
      },
    );

    expect(code).toContain('const value = $ctx.$body.name;');
    expect(update).toHaveBeenCalledTimes(1);
    expect(update).toHaveBeenCalledWith('route_handler_definition', 10, {
      compiledCode: code,
    });
  });

  it('does not write when compiledCode is already executable', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const service = new RouteCacheService({
      queryBuilderService: { update } as any,
      metadataCacheService: {} as any,
      eventEmitter: new EventEmitter2(),
    });

    const code = await (service as any).resolveAndRepairScript(
      'route_handler_definition',
      {
        id: 10,
        scriptLanguage: 'typescript',
        sourceCode: 'return @BODY.name;',
        compiledCode: 'return $ctx.$body.name;',
      },
    );

    expect(code).toBe('return $ctx.$body.name;');
    expect(update).not.toHaveBeenCalled();
  });
});
