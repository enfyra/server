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
});
