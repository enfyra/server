import {
  compileScriptSource,
  getExecutableScript,
  normalizeFlowStepScriptConfig,
  normalizeScriptPatch,
  normalizeScriptRecord,
  resolveExecutableScript,
} from '@enfyra/kernel';

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

  it('normalizes legacy code patches without mutating the patch object', () => {
    const patch = {
      code: 'const value: string = @BODY.name; return value;',
    };
    const normalized = normalizeScriptPatch('pre_hook_definition', patch);

    expect(patch).toEqual({
      code: 'const value: string = @BODY.name; return value;',
    });
    expect(normalized.code).toBeUndefined();
    expect(normalized.sourceCode).toBe(
      'const value: string = @BODY.name; return value;',
    );
    expect(normalized.scriptLanguage).toBe('typescript');
    expect(normalized.compiledCode).toContain(
      'const value = $ctx.$body.name;',
    );
  });

  it('recompiles from existing source when only scriptLanguage is patched', () => {
    const normalized = normalizeScriptPatch(
      'route_handler_definition',
      { scriptLanguage: 'javascript' },
      {
        sourceCode: 'return @BODY.name;',
        compiledCode: 'stale',
        scriptLanguage: 'typescript',
      },
    );

    expect(normalized.sourceCode).toBeUndefined();
    expect(normalized.scriptLanguage).toBe('javascript');
    expect(normalized.compiledCode).toBe('return $ctx.$body.name;');
  });

  it('clears compiled code when source is explicitly cleared', () => {
    const normalized = normalizeScriptPatch(
      'post_hook_definition',
      { sourceCode: null },
      {
        sourceCode: 'return @BODY.name;',
        compiledCode: 'return $ctx.$body.name;',
        scriptLanguage: 'typescript',
      },
    );

    expect(normalized.sourceCode).toBeNull();
    expect(normalized.compiledCode).toBeNull();
  });

  it('does not rewrite invalid JSON flow configs', () => {
    const record = { type: 'script', config: '{broken json' };
    expect(normalizeFlowStepScriptConfig(record)).toBe(record);
  });

  it('normalizes JSON string flow configs and removes legacy code', () => {
    const normalized = normalizeFlowStepScriptConfig({
      type: 'condition',
      config: JSON.stringify({
        code: 'const ok: boolean = @BODY.enabled; return ok;',
      }),
    });

    expect(typeof normalized.config).toBe('string');
    const config = JSON.parse(normalized.config);
    expect(config.code).toBeUndefined();
    expect(config.sourceCode).toContain('@BODY.enabled');
    expect(config.compiledCode).toContain('$ctx.$body.enabled');
  });
});
