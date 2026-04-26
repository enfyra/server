import {
  compileScriptSource,
  getExecutableScript,
  normalizeFlowStepScriptConfig,
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../src/domain/shared/script-code.util';

describe('script-code util', () => {
  it('compiles TypeScript source into executable JavaScript', () => {
    const compiled = compileScriptSource(
      'const value: string = @BODY.name; return value;',
      'typescript',
    );

    expect(compiled).toContain('const value = $ctx.$body.name;');
    expect(compiled).toContain('return value;');
    expect(compiled).not.toContain(': string');
  });

  it('defaults script records to TypeScript and removes legacy fields', () => {
    const record = normalizeScriptRecord('route_handler_definition', {
      logic: 'return await @REPOS.main.find();',
    });

    expect(record.scriptLanguage).toBe('typescript');
    expect(record.sourceCode).toBe('return await @REPOS.main.find();');
    expect(record.compiledCode).toBe('return await $ctx.$repos.main.find();');
    expect(record.logic).toBeUndefined();
  });

  it('normalizes flow script config into source and compiled code', () => {
    const record = normalizeFlowStepScriptConfig({
      type: 'script',
      config: {
        code: 'const id: string = @FLOW_PAYLOAD.id; return id;',
      },
    });

    expect(record.config.scriptLanguage).toBe('typescript');
    expect(record.config.sourceCode).toContain('@FLOW_PAYLOAD.id');
    expect(record.config.compiledCode).toContain('$ctx.$flow.$payload.id');
    expect(record.config.code).toBeUndefined();
  });

  it('executes sourceCode over stale compiledCode', () => {
    const executable = getExecutableScript({
      scriptLanguage: 'typescript',
      sourceCode: 'const value: string = @BODY.name; return value;',
      compiledCode: 'const value: string = $ctx.$body.name; return value;',
    });

    expect(executable).toContain('const value = $ctx.$body.name;');
    expect(executable).not.toContain(': string');
  });

  it('keeps valid compiledCode without repair', () => {
    const resolved = resolveExecutableScript({
      scriptLanguage: 'typescript',
      sourceCode: 'return @BODY.name;',
      compiledCode: 'return $ctx.$body.name;',
    });

    expect(resolved.code).toBe('return $ctx.$body.name;');
    expect(resolved.shouldPersistCompiledCode).toBe(false);
  });

  it('marks stale compiledCode for persistence after fallback compile', () => {
    const resolved = resolveExecutableScript({
      scriptLanguage: 'typescript',
      sourceCode: 'const value: string = @BODY.name; return value;',
      compiledCode: 'const value: string = $ctx.$body.name; return value;',
    });

    expect(resolved.code).toContain('const value = $ctx.$body.name;');
    expect(resolved.compiledCode).toBe(resolved.code);
    expect(resolved.shouldPersistCompiledCode).toBe(true);
  });
});
