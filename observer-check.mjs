import { IsolatedExecutorService } from '@enfyra/kernel';
import { mkdtempSync, writeFileSync } from 'fs';
import { tmpdir } from 'os';
import path from 'path';

const dir = mkdtempSync(path.join(tmpdir(), 'obs-'));
const modulePath = path.join(dir, 'sse-package.mjs');
writeFileSync(modulePath, `
export default {
  createSseStream() {
    return {
      async *[Symbol.asyncIterator]() {
        yield 'data: a\\n\\n';
        yield 'data: usage\\n\\n';
      },
    };
  },
};
`);

const service = new IsolatedExecutorService({
  packageCacheService: { getPackages: async () => ['sse-package'] },
  packageCdnLoaderService: {
    getPackageSources: () => [{ name: 'sse-package', safeName: 'sse_package', version: '1.0.0', sourceCode: '', filePath: modulePath, fileUrl: modulePath }],
  },
});

const seenOptions = [];
const ctx = {
  $body: {}, $query: {}, $params: {}, $share: { $logs: [] },
  $helpers: {}, $cache: {}, $repos: {}, $user: null,
  $res: {
    stream: (stream, options) => new Promise((resolve, reject) => {
      seenOptions.push(options);
      stream.on('data', () => {});
      stream.on('end', () => resolve());
      stream.on('error', reject);
    }),
  },
};

try {
  const result = await service.run(`
    const pkg = $ctx.$pkgs['sse-package'];
    const stream = pkg.createSseStream();
    const observed = [];
    await $ctx.$res.stream(stream, {
      mimetype: 'text/event-stream',
      observer: (text, kind) => { observed.push({ text, kind }); },
    });
    return { observed };
  `, ctx, 5000);
  console.log('RESULT:', JSON.stringify(result));
  console.log('MAIN_OPTIONS:', JSON.stringify(seenOptions));
} catch (e) {
  console.log('ERROR:', e.message);
  console.log('MAIN_OPTIONS:', JSON.stringify(seenOptions));
} finally {
  service.onDestroy();
}
process.exit(0);
