import { executeSingle } from '../helpers/spawn-worker';

const snapshot = {
  $body: {},
  $query: {},
  $params: {},
  $user: null,
  $share: {},
  $api: { request: {} },
};

describe('$pkgs worker proxy fallback', () => {
  it('proxies package function calls through the executor-side package runtime when isolate loading fails', async () => {
    const result = await executeSingle({
      code: `
        const crypto = require('node:crypto');
        const id = await crypto.randomUUID();
        return /^[0-9a-f-]{36}$/.test(id);
      `,
      pkgSources: [
        {
          name: 'node:crypto',
          safeName: 'node_crypto',
          sourceCode: 'import "node:crypto"; export default {};',
        },
      ],
      snapshot,
      ctx: { $share: {} },
    });

    expect(result.value).toBe(true);
  });

  it('proxies constructors and instance method calls through the executor-side package runtime', async () => {
    const result = await executeSingle({
      code: `
        const { URL } = require('node:url');
        const url = new URL('/docs', 'https://enfyra.app');
        return await url.toString();
      `,
      pkgSources: [
        {
          name: 'node:url',
          safeName: 'node_url',
          sourceCode: 'import "node:url"; export default {};',
        },
      ],
      snapshot,
      ctx: { $share: {} },
    });

    expect(result.value).toBe('https://enfyra.app/docs');
  });

  it('keeps returned function handles and async callbacks callable', async () => {
    const result = await executeSingle({
      code: `
        const util = require('node:util');
        const callbackified = await util.callbackify(async (value) => value * 2);
        return await new Promise((resolve, reject) => {
          callbackified(21, (error, value) => error ? reject(error) : resolve(value));
        });
      `,
      pkgSources: [
        {
          name: 'node:util',
          safeName: 'node_util',
          sourceCode: 'import "node:util"; export default {};',
        },
      ],
      snapshot,
      ctx: { $share: {} },
    });

    expect(result.value).toBe(42);
  });

  it('rejects duplicate package names and safe names before execution', async () => {
    await expect(
      executeSingle({
        code: 'return true;',
        pkgSources: [
          { name: 'same', safeName: 'first', sourceCode: 'export default {};' },
          { name: 'same', safeName: 'second', sourceCode: 'export default {};' },
        ],
        snapshot,
      }),
    ).rejects.toThrow('Duplicate executor package name: same');

    await expect(
      executeSingle({
        code: 'return true;',
        pkgSources: [
          { name: 'first', safeName: 'same', sourceCode: 'export default {};' },
          { name: 'second', safeName: 'same', sourceCode: 'export default {};' },
        ],
        snapshot,
      }),
    ).rejects.toThrow('Duplicate executor package safeName: same');
  });

  it('rejects invalid package safe names before execution', async () => {
    await expect(
      executeSingle({
        code: 'return true;',
        pkgSources: [
          {
            name: 'unsafe',
            safeName: 'unsafe-name',
            sourceCode: 'export default {};',
          },
        ],
        snapshot,
      }),
    ).rejects.toThrow('Invalid package safeName: unsafe-name');
  });

  it('ignores package names inside comments and strings during isolate discovery', async () => {
    const result = await executeSingle({
      code: `
        // require('node:crypto')
        const marker = "require('node:crypto')";
        const id = await $ctx.$pkgs['node:crypto'].randomUUID();
        return marker.length > 0 && /^[0-9a-f-]{36}$/.test(id);
      `,
      pkgSources: [
        {
          name: 'node:crypto',
          safeName: 'node_crypto',
          sourceCode:
            'throw new Error("package text was evaluated"); export default {};',
        },
      ],
      snapshot,
    });

    expect(result.value).toBe(true);
  });

  it.each([
    ['number', '42', 42],
    ['string', "'ready'", 'ready'],
    ['boolean', 'false', false],
    ['null', 'null', null],
  ])('transfers %s ESM default exports through require', async (_type, source, expected) => {
    const result = await executeSingle({
      code: `
        const value = require('primitive-default');
        return { value, type: typeof value };
      `,
      pkgSources: [
        {
          name: 'primitive-default',
          safeName: 'primitive_default',
          sourceCode: `export default ${source};`,
        },
      ],
      snapshot,
    });

    expect(result.value).toEqual({ value: expected, type: typeof expected });
  });

  it('propagates fatal ESM evaluation errors instead of proxying the package', async () => {
    await expect(
      executeSingle({
        code: 'require("evaluation-failure"); return true;',
        pkgSources: [
          {
            name: 'evaluation-failure',
            safeName: 'evaluation_failure',
            sourceCode:
              'throw new Error("package evaluation failed"); export default {};',
          },
        ],
        snapshot,
      }),
    ).rejects.toThrow('package evaluation failed');
  });
});
