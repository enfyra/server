import { transformCode } from 'src/kernel/execution';

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

  it('does not expand inside line comment', () => {
    expect(transformCode('// @BODY')).toBe('// @BODY');
  });

  it('does not expand inside block comment', () => {
    expect(transformCode('/* @BODY */')).toBe('/* @BODY */');
  });

  it('expands @ERROR and @STATUS macros', () => {
    expect(transformCode('if (@ERROR) @STATUS')).toBe(
      'if ($ctx.$error) $ctx.$statusCode',
    );
  });

  it('expands @THROW macros', () => {
    expect(transformCode('@THROW400("bad")')).toBe(`$ctx.$throw['400']("bad")`);
  });
});
