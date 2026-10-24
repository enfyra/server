import { Script, runInNewContext } from 'node:vm';
import { expect, it } from 'vitest';
import { assertValidJsBundleSyntax } from '../../src/modules/extension-definition/utils/validation.util';
import { registerExtensionRoutes } from '../../src/http/routes/extension.routes';

it.each(['export default { name: "Page" };', 'module.exports = { name: "Page" };'])('rejects a module source that cannot execute as a classic extension bundle: %s', code => {
  expect(() => assertValidJsBundleSyntax(code)).toThrow(/classic|global|module/i);
});

it('accepts valid global bundles with bracket text inside strings and rejects malformed syntax', () => {
  expect(() => assertValidJsBundleSyntax('window.page = { text: "{" };')).not.toThrow();
  expect(() => assertValidJsBundleSyntax('window.page = { broken: };')).toThrow();
});

it.each([42, '../unsafe name'])('returns the same canonical preview identity the compiler exports (%s)', async id => {
  let preview: any;
  const app = { post: (path: string, fn: unknown) => { if (path.endsWith('/preview')) preview = fn; }, patch: () => {} };
  registerExtensionRoutes(app as any, {} as any);
  let body: any;
  await preview({ body: { id, code: '<template><div>Hello</div></template>' } }, { json: (value: unknown) => { body = value; } });
  const window: Record<string, unknown> = { Vue: { defineComponent: (value: unknown) => value } };
  window.window = window;
  new Script(body.compiledCode);
  runInNewContext(body.compiledCode, window);
  expect(typeof body.extensionId).toBe('string');
  expect(window[body.extensionId]).toBeTruthy();
}, 30000);
