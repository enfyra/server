import { RuntimeScriptRepairService } from '../../../src/engines/cache';

describe('runtime script repair service', () => {
  it('persists repaired compiledCode only when cached compiledCode is invalid', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const service = new RuntimeScriptRepairService({
      queryBuilderService: { update } as any,
    });

    const code = await service.repairScriptRecord('enfyra_route_handler', {
      id: 10,
      scriptLanguage: 'typescript',
      sourceCode: 'const value: string = @BODY.name; return value;',
      compiledCode: 'const value: string = $ctx.$body.name; return value;',
    });

    expect(code).toContain('const value = $ctx.$body.name;');
    expect(update).toHaveBeenCalledTimes(1);
    expect(update).toHaveBeenCalledWith('enfyra_route_handler', 10, {
      compiledCode: code,
    });
  });

  it('does not write when compiledCode is already executable', async () => {
    const update = vi.fn().mockResolvedValue(undefined);
    const service = new RuntimeScriptRepairService({
      queryBuilderService: { update } as any,
    });

    const code = await service.repairScriptRecord('enfyra_route_handler', {
      id: 10,
      scriptLanguage: 'typescript',
      sourceCode: 'return @BODY.name;',
      compiledCode: 'return $ctx.$body.name;',
    });

    expect(code).toBe('return $ctx.$body.name;');
    expect(update).not.toHaveBeenCalled();
  });
});
