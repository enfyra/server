import * as kernel from '@enfyra/kernel';
import {
  assertScriptSourceContract,
  compileScriptSource,
} from '../../src/shared/utils/script-code.util';
import { transformTemplateSyntax as transformCode } from '../../src/shared/utils/template-syntax.util';

describe('transformCode', () => {
  it('expands @BODY in normal code', () => {
    expect(transformCode('const x = @BODY;')).toBe('const x = $ctx.$body;');
  });

  it('does not expand @BODY inside double-quoted string', () => {
    expect(transformCode('"@BODY"')).toBe('"@BODY"');
  });

  it('does not expand @BODY inside single-quoted string', () => {
    expect(transformCode("'@BODY'")).toBe("'@BODY'");
  });

  it('does not expand @BODY in template literal text portion', () => {
    const input = '`hello @BODY world`';
    expect(transformCode(input)).toBe('`hello @BODY world`');
  });

  it('expands @BODY inside template literal ${} expression', () => {
    const input = '`value: ${@BODY}`';
    expect(transformCode(input)).toBe('`value: ${$ctx.$body}`');
  });

  it('expands @BODY inside nested template literal ${} expression', () => {
    const input = '`${@BODY.name} - ${@QUERY.page}`';
    expect(transformCode(input)).toBe(
      '`${$ctx.$body.name} - ${$ctx.$query.page}`',
    );
  });

  it('handles ${} with braces inside expression', () => {
    const input = '`${(() => { return @BODY; })()}`';
    const result = transformCode(input);
    expect(result).toContain('$ctx.$body');
    expect(result).toContain('`${');
  });

  it('handles mixed code + template literal', () => {
    const input =
      'const x = @BODY; const y = `msg: ${@QUERY}`; const z = @DATA;';
    const result = transformCode(input);
    expect(result).toContain('$ctx.$body');
    expect(result).toContain('${$ctx.$query}');
    expect(result).toContain('$ctx.$data');
  });

  it('expands @ENV to sanitized environment context', () => {
    expect(transformCode('const nodeName = @ENV.NODE_NAME;')).toBe(
      'const nodeName = $ctx.$env.NODE_NAME;',
    );
  });

  it('expands @STORAGE to dynamic storage helpers', () => {
    expect(
      transformCode('return @STORAGE.$upload({ file: @UPLOADED_FILE });'),
    ).toBe('return $ctx.$storage.$upload({ file: $ctx.$uploadedFile });');
  });

  it('expands @TRANSACTION to the dynamic transaction facade', () => {
    expect(transformCode('await @TRANSACTION.run(async () => {});')).toBe(
      'await $ctx.$transaction.run(async () => {});',
    );
  });

  it('does not expand inside line comment', () => {
    expect(transformCode('// @BODY')).toBe('// @BODY');
  });

  it('does not expand inside block comment', () => {
    expect(transformCode('/* @BODY */')).toBe('/* @BODY */');
  });

  it('does not expand macro-like text after a regular expression containing quotes', () => {
    const input = [
      `const escaped = value.replace(/\\\"/g, '&quot;');`,
      `const html = '<table width="100%" style="color:#33434d">@BODY %pkg #repo</table>';`,
      'return { body: @BODY, repo: #projects, pkg: %resend };',
    ].join('\n');

    expect(transformCode(input)).toBe(
      [
        `const escaped = value.replace(/\\\"/g, '&quot;');`,
        `const html = '<table width="100%" style="color:#33434d">@BODY %pkg #repo</table>';`,
        'return { body: $ctx.$body, repo: $ctx.$repos.projects, pkg: $ctx.$pkgs.resend };',
      ].join('\n'),
    );
  });

  it('keeps regular expression bodies and ordinary modulo operators literal', () => {
    const input = `const pattern = /["'#%@]/g; const remainder = total % count; return @BODY;`;

    expect(transformCode(input)).toBe(
      `const pattern = /["'#%@]/g; const remainder = total % count; return $ctx.$body;`,
    );
  });

  it('locates regular expressions safely in macro-heavy script syntax', () => {
    const input = [
      'const body = @BODY;',
      'const repo = #secure.projects;',
      'const pkg = %resend;',
      `const pattern = /["'#%@]/g;`,
      'return { body, repo, pkg, pattern };',
    ].join('\n');

    expect(() => transformCode(input)).not.toThrow();
    expect(transformCode(input)).toContain(`const pattern = /["'#%@]/g;`);
  });

  it('expands @ERROR and @STATUS macros', () => {
    expect(transformCode('if (@ERROR) @STATUS')).toBe(
      'if ($ctx.$error) $ctx.$statusCode',
    );
  });

  it('lowers status throw aliases to the canonical HTTP throw', () => {
    const aliases = [400, 401, 403, 404, 409, 422, 429, 500, 503];

    for (const statusCode of aliases) {
      expect(transformCode(`@THROW${statusCode}("Request failed")`)).toBe(
        `$ctx.$throw.http(${statusCode}, "Request failed")`,
      );
    }
    expect(transformCode('@THROW.http(502, "Upstream request failed")')).toBe(
      '$ctx.$throw.http(502, "Upstream request failed")',
    );
    expect(transformCode('@THROW403 ("Access denied")')).toBe(
      '$ctx.$throw.http(403, "Access denied")',
    );
  });

  it('accepts only the canonical source-level throw contract', () => {
    expect(() => assertScriptSourceContract('@THROW.http(502, "Upstream request failed");')).not.toThrow();
    expect(() => assertScriptSourceContract('@THROW.json({ error: { code: "upstream_error" } }, { statusCode: 502, headers: { "x-should-retry": "true" } });')).not.toThrow();
    expect(() => assertScriptSourceContract('@THROW.json({ reason: "conflict" }, { statusCode: 409 });')).not.toThrow();
    expect(() => assertScriptSourceContract('@THROW.json(errorBody, { statusCode: 500 });')).not.toThrow();
    expect(() => assertScriptSourceContract('@THROW.json({ error: errorDetails }, { statusCode: 500 });')).not.toThrow();
    expect(() => assertScriptSourceContract('@THROW.json([], { statusCode: 500 });')).toThrow(
      '$throw.json body must be a JSON object',
    );
    expect(() => assertScriptSourceContract('@THROW.json({ error: "failed" }, { statusCode: 500 });')).toThrow(
      '$throw.json body.error must be a JSON object',
    );
    expect(() => assertScriptSourceContract('@THROW.json({ success: true, error: {} }, { statusCode: 500 });')).toThrow(
      '$throw.json body.success and body.statusCode are server-owned',
    );
    expect(() => assertScriptSourceContract('@THROW.json({ statusCode: 200, error: {} }, { statusCode: 500 });')).toThrow(
      '$throw.json body.success and body.statusCode are server-owned',
    );
    expect(() => assertScriptSourceContract('@THROW.json({ error: { statusCode: 200 } }, { statusCode: 500 });')).toThrow(
      '$throw.json body.error.statusCode is not allowed; use options.statusCode',
    );
    expect(() => assertScriptSourceContract('@THROW404("Project not found");')).not.toThrow();
    expect(() => assertScriptSourceContract('@THROW400();')).toThrow(
      '@THROW status aliases require exactly one message',
    );
    expect(() => assertScriptSourceContract('@THROW400("Invalid request", { field: "email" });')).toThrow(
      '@THROW status aliases require exactly one message',
    );
    expect(() => assertScriptSourceContract('@THROW.error(502, {});')).toThrow(
      '$throw exposes only .http(statusCode, message?) and .json(body, options?)',
    );
    expect(() => assertScriptSourceContract('$ctx.$throw.notFound("Project");')).toThrow(
      '$throw exposes only .http(statusCode, message?) and .json(body, options?)',
    );
    expect(() => assertScriptSourceContract('$ctx.$throw["404"]("missing");')).toThrow(
      '$throw exposes only .http(statusCode, message?) and .json(body, options?)',
    );
    expect(() => assertScriptSourceContract('@THROW.http(502, "bad", { retry: true });')).toThrow(
      '$throw.http accepts only statusCode and optional message',
    );
    expect(() => assertScriptSourceContract('$ctx.$throw.http.call(null, 502);')).toThrow(
      '$throw exposes only .http(statusCode, message?) and .json(body, options?)',
    );
    expect(() => assertScriptSourceContract('$ctx["$throw"].http(502);')).toThrow(
      '$throw exposes only .http(statusCode, message?) and .json(body, options?)',
    );
    expect(() => assertScriptSourceContract('const thrower = $ctx.$throw;')).toThrow(
      '$throw exposes only .http(statusCode, message?) and .json(body, options?)',
    );
  });

  it('expands repository shorthand', () => {
    expect(transformCode('return await #projects.find({ limit: 1 });')).toBe(
      'return await $ctx.$repos.projects.find({ limit: 1 });',
    );
  });

  it('expands secure repository shorthand through property access', () => {
    expect(
      transformCode('return await #secure.projects.find({ limit: 1 });'),
    ).toBe('return await $ctx.$repos.secure.projects.find({ limit: 1 });');
  });

  it('keeps runtime transform ownership in ESV instead of the kernel', () => {
    expect((kernel as any).transformCode).toBeUndefined();
    expect(compileScriptSource('return @BODY.name;', 'javascript')).toBe(
      'return $ctx.$body.name;',
    );
    expect(() =>
      kernel.compileScriptSource('return @BODY.name;', 'javascript'),
    ).toThrow();
  });
});
