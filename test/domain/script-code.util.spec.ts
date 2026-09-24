import {
  compileScriptSource,
  normalizeFlowStepScriptConfig,
  normalizeScriptPatch,
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../src/shared/utils/script-code.util';
import { SCRIPT_TABLE_NAMES } from '../../src/shared/utils/script-table-contract.constants';

describe('script-code util', () => {
  it('validates JavaScript without executing the source', () => {
    expect(compileScriptSource('return 42;', 'javascript')).toBe('return 42;');
    expect(() => compileScriptSource('const value = ;', 'javascript')).toThrow();
  });

  it('compiles TypeScript source into executable JavaScript', () => {
    const compiled = compileScriptSource(
      'const value: string = @BODY.name; return value;',
      'typescript',
    );

    expect(compiled).toContain('const value = $ctx.$body.name;');
    expect(compiled).toContain('return value;');
    expect(compiled).not.toContain(': string');
  });

  it.each(SCRIPT_TABLE_NAMES)(
    'preserves regex and email markup when compiling %s',
    (tableName) => {
      const sourceCode = [
        `const escaped = value.replace(/\\\"/g, '&quot;');`,
        `const html = '<table width="100%" style="color:#33434d">@BODY</table>';`,
        'return { html, body: @BODY };',
      ].join('\n');

      const record = normalizeScriptRecord(tableName, {
        sourceCode,
        scriptLanguage: 'javascript',
      });

      expect(record.compiledCode).toContain(
        `'<table width="100%" style="color:#33434d">@BODY</table>'`,
      );
      expect(record.compiledCode).toContain('body: $ctx.$body');
      expect(record.compiledCode).not.toContain('$ctx.$pkgs.');
      expect(record.compiledCode).not.toContain('$ctx.$repos.33434d');
    },
  );

  it('ignores inherited script table names', () => {
    const record = {
      logic: 'return 1;',
      sourceCode: 'unchanged',
    };

    expect(normalizeScriptRecord('toString', record)).toBe(record);
  });

  it('defaults script records to TypeScript and removes legacy fields', () => {
    const record = normalizeScriptRecord('enfyra_route_handler', {
      logic: 'return await @REPOS.main.find();',
    });

    expect(record.scriptLanguage).toBe('typescript');
    expect(record.sourceCode).toBe('return await @REPOS.main.find();');
    expect(record.compiledCode).toBe('return await $ctx.$repos.main.find();');
    expect(record.logic).toBeUndefined();
  });

  it('normalizes flow script fields into source and compiled code', () => {
    const record = normalizeFlowStepScriptConfig({
      type: 'script',
      config: {
        code: 'const id: string = @FLOW_PAYLOAD.id; return id;',
      },
    });

    expect(record.scriptLanguage).toBe('typescript');
    expect(record.sourceCode).toContain('@FLOW_PAYLOAD.id');
    expect(record.compiledCode).toContain('$ctx.$flow.$payload.id');
    expect(record.config.code).toBeUndefined();
    expect(record.config.sourceCode).toBeUndefined();
  });

  it('executes sourceCode over stale compiledCode', () => {
    const executable = resolveExecutableScript({
      scriptLanguage: 'typescript',
      sourceCode: 'const value: string = @BODY.name; return value;',
      compiledCode: 'const value: string = $ctx.$body.name; return value;',
    }).code;

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

  it('uses an own non-empty legacy source field', () => {
    const inherited = Object.create({ logic: 'return "inherited";' });
    inherited.code = '';
    inherited.handlerScript = 'return "own";';

    const resolved = resolveExecutableScript(inherited);

    expect(resolved.code).toBe('return "own";');
  });

  it('normalizes legacy code patches without mutating the patch object', () => {
    const patch = {
      code: 'const value: string = @BODY.name; return value;',
    };
    const normalized = normalizeScriptPatch('enfyra_pre_hook', patch);

    expect(patch).toEqual({
      code: 'const value: string = @BODY.name; return value;',
    });
    expect(normalized.code).toBeUndefined();
    expect(normalized.sourceCode).toBe(
      'const value: string = @BODY.name; return value;',
    );
    expect(normalized.scriptLanguage).toBe('typescript');
    expect(normalized.compiledCode).toContain('const value = $ctx.$body.name;');
  });

  it('recompiles from existing source when only scriptLanguage is patched', () => {
    const normalized = normalizeScriptPatch(
      'enfyra_route_handler',
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
      'enfyra_post_hook',
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

  it('preserves explicit source clearing when a legacy field is also present', () => {
    const normalized = normalizeScriptPatch(
      'enfyra_pre_hook',
      {
        sourceCode: null,
        code: 'return @BODY.name;',
      },
      {
        sourceCode: 'return @BODY.oldName;',
        scriptLanguage: 'typescript',
      },
    );

    expect(normalized.sourceCode).toBeNull();
    expect(normalized.compiledCode).toBeNull();
    expect(normalized.code).toBeUndefined();
  });

  it('requires existing source when only scriptLanguage changes', () => {
    expect(() => normalizeScriptPatch(
      'enfyra_route_handler',
      { scriptLanguage: 'javascript' },
    )).toThrow('Existing script data is required');
  });

  it('drops direct compiledCode patches for script tables', () => {
    const normalized = normalizeScriptPatch(
      'enfyra_route_handler',
      { compiledCode: 'return "forged";' },
      {
        sourceCode: 'return @BODY.name;',
        compiledCode: 'return $ctx.$body.name;',
        scriptLanguage: 'typescript',
      },
    );

    expect(normalized.compiledCode).toBeUndefined();
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
    expect(config.sourceCode).toBeUndefined();
    expect(normalized.sourceCode).toContain('@BODY.enabled');
    expect(normalized.compiledCode).toContain('$ctx.$body.enabled');
  });
});
