import { Worker } from 'node:worker_threads';

const workerScript =
  process.env.ENFYRA_KERNEL_WORKER_SCRIPT
  || require.resolve('@enfyra/kernel/execution/worker.js');

async function executeWithFallbacks(
  code: string,
  missingGlobals = [
    'TextDecoder',
    'FormData',
    'URLSearchParams',
    'URL',
  ],
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const fallbackSetup = missingGlobals
      .map((name) => `globalThis[${JSON.stringify(name)}] = undefined;`)
      .join('\n');
    const worker = new Worker(
      `
        ${fallbackSetup}
        require(${JSON.stringify(workerScript)});
      `,
      { eval: true },
    );
    const id = `fallback_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    let result: any;
    let settled = false;
    const timer = setTimeout(async () => {
      if (settled) return;
      settled = true;
      await worker.terminate();
      reject(new Error('Fallback executor test timed out'));
    }, 10_000);

    const finish = async () => {
      worker.postMessage({ type: 'shutdown' });
      const shutdownTimer = setTimeout(() => {
        void worker.terminate();
      }, 1_000);
      await new Promise<void>((done) => {
        worker.once('exit', () => done());
      });
      clearTimeout(shutdownTimer);
    };

    worker.on('message', async (message) => {
      if (message.type === 'helpersCall') {
        const args = JSON.parse(message.argsJson || '[]');
        let value: unknown = true;
        if (message.name === 'inspectFileForm') {
          const file = args[0]?.__formData?.[0]?.[1]?.__file;
          value = {
            name: file?.name,
            type: file?.type,
            lastModified: file?.lastModified,
            data: file?.data,
          };
        }
        worker.postMessage({
          type: 'callResult',
          callId: message.callId,
          result: JSON.stringify({ __e: 'v', d: value }),
        });
        return;
      }
      if (message.type === 'result') {
        result = message;
        return;
      }
      if (message.type !== 'taskReleased' || message.id !== id || settled) return;
      settled = true;
      clearTimeout(timer);
      await finish();
      if (result?.success) resolve(result.value);
      else reject(new Error(result?.error?.message || 'Fallback execution failed'));
    });
    worker.on('error', async (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      await worker.terminate();
      reject(error);
    });
    worker.on('exit', (exitCode) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`Fallback worker exited with code ${exitCode}`));
    });

    worker.postMessage({
      type: 'execute',
      id,
      code,
      sourceCode: code,
      pkgSources: [],
      snapshot: {
        $body: {},
        $query: {},
        $params: {},
        $share: {},
        $api: { request: {} },
      },
      timeoutMs: 5_000,
      memoryLimitMb: 128,
      isolatePoolSize: 1,
      tasksPerIsolate: 1,
    });
  });
}

describe('executor fallback globals', () => {
  it('matches streaming UTF-8 replacement and BOM behavior', async () => {
    const result = await executeWithFallbacks(`
      const malformed = new TextDecoder();
      const first = malformed.decode(Uint8Array.from([0xe2]), { stream: true });
      const second = malformed.decode(Uint8Array.from([0x28]), { stream: true });
      const flushed = malformed.decode();
      const truncated = new TextDecoder().decode(Uint8Array.from([0xe2, 0x82]));
      const overlong = new TextDecoder().decode(Uint8Array.from([0xe0, 0x80, 0x80]));
      const bom = new TextDecoder().decode(Uint8Array.from([0xef, 0xbb, 0xbf, 0x61]));
      let fatal = null;
      try {
        new TextDecoder('utf-8', { fatal: true }).decode(Uint8Array.from([0xff]));
      } catch (error) {
        fatal = error.name;
      }
      let unsupported = null;
      try {
        new TextDecoder('utf-16');
      } catch (error) {
        unsupported = error.name;
      }
      const destination = new Uint8Array(3);
      const encoded = new TextEncoder().encodeInto('aé', destination);
      return {
        first,
        second,
        flushed,
        truncated,
        overlong,
        bom,
        fatal,
        unsupported,
        encoded,
        destination: Array.from(destination),
      };
    `);

    expect(result).toEqual({
      first: '',
      second: '�(',
      flushed: '',
      truncated: '�',
      overlong: '���',
      bom: 'a',
      fatal: 'TypeError',
      unsupported: 'RangeError',
      encoded: { read: 2, written: 3 },
      destination: [97, 195, 169],
    });
  });

  it('matches form coercion and URLSearchParams serialization', async () => {
    const result = await executeWithFallbacks(`
      const object = { marker: 1 };
      const form = new FormData();
      form.append('value', object);
      form.append('value', 2);
      object.marker = 2;
      const formValues = form.getAll('value');
      const formKeys = Array.from(form.keys());
      form.set('value', false);
      const formSetValue = form.get('value');
      form.delete('value');
      const formHasValue = form.has('value');
      const params = new URLSearchParams({ a: 'hello world', b: "!~'()" });
      params.append('a', 'again');
      const paramPairs = Array.from(params.entries());
      const paramValues = params.getAll('a');
      const malformed = new URLSearchParams('q=%').get('q');
      const surrogate = new URLSearchParams();
      surrogate.append('x', '\ud800');
      return {
        formValue: formValues[0],
        formValues,
        formKeys,
        formSetValue,
        formHasValue,
        params: params.toString(),
        paramPairs,
        paramValues,
        malformed,
        surrogate: surrogate.toString(),
      };
    `);

    expect(result).toEqual({
      formValue: '[object Object]',
      formValues: ['[object Object]', '2'],
      formKeys: ['value', 'value'],
      formSetValue: 'false',
      formHasValue: false,
      params: 'a=hello+world&b=%21%7E%27%28%29&a=again',
      paramPairs: [
        ['a', 'hello world'],
        ['b', "!~'()"],
        ['a', 'again'],
      ],
      paramValues: ['hello world', 'again'],
      malformed: '%',
      surrogate: 'x=%EF%BF%BD',
    });
  });

  it('preserves fallback File metadata through FormData host arguments', async () => {
    const result = await executeWithFallbacks(
      `
        const form = new FormData();
        form.append(
          'upload',
          new Blob([Uint8Array.from([1, 2, 3])], { type: 'image/png' }),
          'image.png',
        );
        return await $ctx.$helpers.inspectFileForm(form);
      `,
      ['FormData', 'File'],
    );

    expect(result).toEqual({
      name: 'image.png',
      type: 'image/png',
      lastModified: 0,
      data: [1, 2, 3],
    });
  });

  it('keeps fallback URL live with a native URLSearchParams implementation', async () => {
    const result = await executeWithFallbacks(
      `
        const url = new URL('https://example.test/path?a=1');
        url.searchParams.set('a', '2');
        url.searchParams.append('b', '3');
        url.searchParams.sort();
        URLSearchParams.prototype.append.call(url.searchParams, 'c', '4');
        const borrowedHref = url.href;
        url.searchParams.append = () => { throw new Error('must not run'); };
        url.href = 'https://example.test/next?d=5';
        const ownKeys = Reflect.ownKeys(url);
        return {
          borrowedHref,
          href: url.href,
          search: url.search,
          params: Array.from(url.searchParams.entries()),
          hasState: '__state' in url,
          ownKeys,
        };
      `,
      ['URL'],
    );

    expect(result).toEqual({
      borrowedHref: 'https://example.test/path?a=2&b=3&c=4',
      href: 'https://example.test/next?d=5',
      search: '?d=5',
      params: [['d', '5']],
      hasState: false,
      ownKeys: ['searchParams'],
    });
  });

  it('resolves URL bases, hides credentials, and keeps searchParams live', async () => {
    const result = await executeWithFallbacks(`
      const url = new URL('/v1', 'https://user:pass@example.test/root');
      const searchParamsIdentity = url.searchParams;
      url.searchParams.append('q', 'a b');
      url.pathname = '/v2';
      url.search = '?x=1';
      url.hash = '#done';
      const fragment = new URL('https://example.test/#section?not=query');
      const child = new URL('child?next=/../admin', 'https://example.test');
      const empty = new URL('', 'https://example.test/a/b?x#y');
      let relativeError = null;
      try {
        new URL('/admin');
      } catch (error) {
        relativeError = error.name;
      }
      return {
        href: url.href,
        host: url.host,
        origin: url.origin,
        protocol: url.protocol,
        pathname: url.pathname,
        search: url.search,
        hash: url.hash,
        json: url.toJSON(),
        stableParams: searchParamsIdentity === url.searchParams,
        params: Array.from(url.searchParams.entries()),
        fragmentSearch: fragment.search,
        child: child.href,
        empty: empty.href,
        relativeError,
      };
    `);

    expect(result).toEqual({
      href: 'https://user:pass@example.test/v2?x=1#done',
      host: 'example.test',
      origin: 'https://example.test',
      protocol: 'https:',
      pathname: '/v2',
      search: '?x=1',
      hash: '#done',
      json: 'https://user:pass@example.test/v2?x=1#done',
      stableParams: true,
      params: [['x', '1']],
      fragmentSearch: '',
      child: 'https://example.test/child?next=/../admin',
      empty: 'https://example.test/a/b?x',
      relativeError: 'TypeError',
    });
  });
});
