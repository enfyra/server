import { PackageCdnLoaderService } from '../src/engines/cache/services/package-cdn-loader.service';
import { IsolatedExecutorService } from '../src/kernel/execution/executor-engine/services/isolated-executor.service';

type PackageCase = {
  name: string;
  version: string;
  code: string;
};

const cases: PackageCase[] = [
  { name: 'lodash-es', version: '4.17.21', code: `return await pkg.camelCase('Hello runtime test') === 'helloRuntimeTest';` },
  { name: 'dayjs', version: '1.11.13', code: `return await pkg('2020-01-02').format('YYYY-MM-DD') === '2020-01-02';` },
  { name: 'date-fns', version: '3.6.0', code: `return await pkg.format(new Date('2020-01-02T00:00:00.000Z'), 'yyyy-MM-dd') === '2020-01-02';` },
  { name: 'uuid', version: '10.0.0', code: `const id = await pkg.v4(); return typeof id === 'string' && id.length === 36;` },
  { name: 'nanoid', version: '5.0.9', code: `return await pkg.nanoid(12).length === 12;` },
  { name: 'slugify', version: '1.6.6', code: `return await pkg('Xin chao runtime') === 'Xin-chao-runtime';` },
  { name: 'pluralize', version: '8.0.0', code: `return await pkg('box', 2) === 'boxes';` },
  { name: 'change-case', version: '5.4.4', code: `return await pkg.camelCase('Hello runtime test') === 'helloRuntimeTest';` },
  { name: 'qs', version: '6.13.0', code: `return await pkg.parse('a=b&c=d').a === 'b' && await pkg.stringify({ a: 'b' }) === 'a=b';` },
  { name: 'query-string', version: '9.1.1', code: `return await pkg.parse('a=b').a === 'b' && await pkg.stringify({ a: 'b' }) === 'a=b';` },
  { name: 'zod', version: '3.25.76', code: `return await pkg.z.string().parse('enfyra') === 'enfyra';` },
  { name: 'mitt', version: '3.0.1', code: `const bus = await pkg(); return typeof bus.on === 'function' && typeof bus.emit === 'function';` },
  { name: 'eventemitter3', version: '5.0.1', code: `const bus = new pkg(); return typeof bus.on === 'function' && typeof bus.emit === 'function';` },
  { name: 'ms', version: '2.1.3', code: `return await pkg('2 days') === 172800000 && await pkg(60000) === '1m';` },
  { name: 'mime-types', version: '2.1.35', code: `return await pkg.lookup('file.json') === 'application/json';` },
  { name: 'validator', version: '13.12.0', code: `return await pkg.isEmail('test@example.com') === true;` },
  { name: 'seedrandom', version: '3.0.5', code: `const rng = await pkg('seed'); return typeof await rng() === 'number';` },
  { name: 'ulid', version: '2.3.0', code: `return await pkg.ulid().length === 26;` },
  { name: 'is-plain-object', version: '5.0.0', code: `return await pkg.isPlainObject({ ok: true }) === true && await pkg.isPlainObject(new Date()) === false;` },
  { name: 'deepmerge', version: '4.3.1', code: `return await pkg({ a: 1, nested: { x: 1 } }, { b: 2, nested: { y: 2 } }).nested.y === 2;` },
  { name: 'fast-json-stable-stringify', version: '2.1.0', code: `return await pkg({ b: 1, a: 2 }) === '{"a":2,"b":1}';` },
  { name: 'just-clone', version: '6.2.0', code: `const a = { nested: { value: 1 } }; const b = await pkg(a); b.nested.value = 2; return a.nested.value === 1;` },
  { name: 'colord', version: '2.9.3', code: `return await pkg.colord('#ff0000').toHex() === '#ff0000';` },
  { name: 'yaml', version: '2.8.1', code: `return await pkg.parse('a: 1\\n').a === 1 && (await pkg.stringify({ b: 2 })).includes('b: 2');` },
  { name: 'decimal.js', version: '10.4.3', code: `const value = new pkg(0.1); return await value.plus(0.2).toString() === '0.3';` },
  { name: 'bignumber.js', version: '9.1.2', code: `const value = new pkg(0.1); return await value.plus(0.2).toString() === '0.3';` },
  { name: 'p-limit', version: '6.1.0', code: `const limit = await pkg(1); const values = await Promise.all([limit(async () => 1), limit(async () => 2)]); return values.join(',') === '1,2';` },
  { name: 'js-base64', version: '3.7.7', code: `return await pkg.Base64.encode('hello') === 'aGVsbG8=' && await pkg.Base64.decode('aGVsbG8=') === 'hello';` },
  { name: 'cookie', version: '0.6.0', code: `return await pkg.parse('a=b').a === 'b' && await pkg.serialize('a', 'b') === 'a=b';` },
  { name: 'he', version: '1.2.0', code: `return await pkg.decode('&amp;') === '&';` },
  { name: 'bcryptjs', version: '2.4.3', code: `const hash = await pkg.hash('secret', 4); return await pkg.compare('secret', hash) === true;` },
  { name: 'jsonwebtoken', version: '9.0.2', code: `const token = await pkg.sign({ sub: '1' }, 'secret'); return await pkg.verify(token, 'secret').sub === '1';` },
  { name: 'crypto-js', version: '4.2.0', code: `return await pkg.SHA256('hello').toString().length === 64;` },
  { name: 'jose', version: '5.9.6', code: `const secret = new TextEncoder().encode('12345678901234567890123456789012'); const token = await new pkg.SignJWT({ sub: '1' }).setProtectedHeader({ alg: 'HS256' }).sign(secret); const verified = await pkg.jwtVerify(token, secret); return await verified.payload.sub === '1';` },
  { name: 'fast-xml-parser', version: '4.5.0', code: `const parser = new pkg.XMLParser(); return await parser.parse('<root><a>1</a></root>').root.a === 1;` },
  { name: 'papaparse', version: '5.4.1', code: `return await pkg.parse('a,b\\n1,2', { header: true }).data[0].a === '1';` },
  { name: 'handlebars', version: '4.7.8', code: `const render = await pkg.compile('Hi {{name}}'); return await render({ name: 'Enfyra' }) === 'Hi Enfyra';` },
  { name: 'mustache', version: '4.2.0', code: `return await pkg.render('Hi {{name}}', { name: 'Enfyra' }) === 'Hi Enfyra';` },
  { name: 'marked', version: '14.1.3', code: `return (await pkg.marked('# Hi')).includes('<h1>Hi</h1>');` },
  { name: 'cheerio', version: '1.0.0', code: `const $ = await pkg.load('<h1>Hi</h1>'); return await $('h1').text() === 'Hi';` },
  { name: 'ajv', version: '8.17.1', code: `const ajv = new pkg(); const validate = await ajv.compile({ type: 'object', properties: { a: { type: 'number' } }, required: ['a'] }); return await validate({ a: 1 }) === true;` },
  { name: 'yup', version: '1.4.0', code: `return await pkg.object({ name: pkg.string().required() }).validate({ name: 'Enfyra' }).name === 'Enfyra';` },
  { name: 'buffer', version: '6.0.3', code: `const Buffer = await pkg.Buffer; const value = await Buffer.from('hello'); return await value.toString('base64') === 'aGVsbG8=';` },
  { name: 'file-type', version: '19.5.0', code: `const bytes = new Uint8Array([0x89,0x50,0x4E,0x47,0x0D,0x0A,0x1A,0x0A,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]); const result = await pkg.fileTypeFromBuffer(bytes); return await result.ext === 'png';` },
  { name: 'node-html-parser', version: '6.1.13', code: `const root = await pkg.parse('<div><span>Hi</span></div>'); return await root.querySelector('span').toString() === '<span>Hi</span>';` },
  { name: 'semver', version: '7.6.3', code: `return await pkg.satisfies('1.2.3', '^1.0.0') === true;` },
  { name: 'ipaddr.js', version: '2.2.0', code: `return await pkg.parse('127.0.0.1').kind() === 'ipv4';` },
  { name: 'minimatch', version: '10.0.1', code: `return await pkg.minimatch('src/app.ts', 'src/**/*.ts') === true;` },
  { name: 'lru-cache', version: '11.0.2', code: `const cache = new pkg.LRUCache({ max: 2 }); await cache.set('a', 1); return await cache.get('a') === 1;` },
  { name: 'form-data', version: '4.0.1', code: `const form = new pkg(); await form.append('name', 'Enfyra'); return typeof await form.getHeaders().get === 'undefined' && await form.getHeaders()['content-type'].startsWith('multipart/form-data');` },
];

function getSelectedCases(): PackageCase[] {
  const args = process.argv.slice(2);
  const onlyArg = args.find((arg) => arg.startsWith('--only='));
  const names = new Set(
    (onlyArg ? onlyArg.slice('--only='.length).split(',') : args)
      .map((name) => name.trim())
      .filter(Boolean),
  );
  if (names.size === 0) return cases;
  return cases.filter((item) => names.has(item.name));
}

function buildScript(selectedCases: PackageCase[], packageNames: string[]): string {
  const lines = [
    `const results = {};`,
    `async function run(name, fn) {`,
    `  try {`,
    `    const pkg = $ctx.$pkgs[name];`,
    `    results[name] = await fn(pkg);`,
    `  } catch (error) {`,
    `    results[name] = { error: error?.message || String(error) };`,
    `  }`,
    `}`,
  ];

  for (const item of selectedCases.filter((candidate) => packageNames.includes(candidate.name))) {
    lines.push(
      `await run(${JSON.stringify(item.name)}, async (pkg) => { ${item.code} });`,
    );
  }
  lines.push(`return results;`);
  return lines.join('\n');
}

async function main() {
  const selectedCases = getSelectedCases();
  if (selectedCases.length === 0) {
    console.error('No package cases matched the requested filter.');
    process.exitCode = 1;
    return;
  }

  const loader = new PackageCdnLoaderService();
  const installed: string[] = [];
  const installFailures: Array<{ name: string; error: string }> = [];

  for (const item of selectedCases) {
    process.stdout.write(`install ${item.name}@${item.version} ... `);
    try {
      await loader.loadPackage(item.name, item.version);
      installed.push(item.name);
      process.stdout.write('ok\n');
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      installFailures.push({ name: item.name, error: message });
      process.stdout.write(`failed: ${message}\n`);
    }
  }

  const service = new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => installed,
    } as any,
    packageCdnLoaderService: loader,
  });
  const ctx: any = {
    $body: {},
    $query: {},
    $params: {},
    $share: { $logs: [] },
    $helpers: {},
    $cache: {},
    $repos: {},
    $user: null,
  };
  ctx.$logs = (...args: any[]) => ctx.$share.$logs.push(...args);

  try {
    const results = await service.run(buildScript(selectedCases, installed), ctx, 60000);
    const failedCases = Object.entries(results)
      .filter(([, value]) => value !== true)
      .map(([name, value]) => ({ name, value }));

    console.log('\nPackage runtime stress result:');
    console.table(
      selectedCases.map((item) => ({
        package: `${item.name}@${item.version}`,
        install: installed.includes(item.name) ? 'ok' : 'failed',
        runtime: results[item.name] === true ? 'ok' : JSON.stringify(results[item.name] ?? null),
      })),
    );

    if (installFailures.length || failedCases.length) {
      process.exitCode = 1;
      if (installFailures.length) {
        console.error('\nInstall failures:', JSON.stringify(installFailures, null, 2));
      }
      if (failedCases.length) {
        console.error('\nRuntime failures:', JSON.stringify(failedCases, null, 2));
      }
    }
  } finally {
    service.onDestroy();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
