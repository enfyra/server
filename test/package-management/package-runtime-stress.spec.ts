import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '../../src/kernel/execution';

let tempDir: string | null = null;

afterEach(async () => {
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
});

async function writePackage(name: string, source: string) {
  if (!tempDir) tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-runtime-stress-'));
  const filePath = path.join(tempDir, `${name}.mjs`);
  await writeFile(filePath, source, 'utf8');
  return {
    name,
    safeName: name.replace(/[^a-zA-Z0-9]/g, '_'),
    version: '1.0.0',
    sourceCode: '',
    filePath,
    fileUrl: filePath,
  };
}

function createContext() {
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
  return ctx;
}

describe('package runtime stress shapes', () => {
  it('supports proxied functions, classes, constructors, instances, this binding, and chained results', async () => {
    const packages = await Promise.all([
      writePackage(
        'fn-default',
        `
          export default function title(input) {
            return String(input).trim().replace(/\\s+/g, '-').toLowerCase();
          }
        `,
      ),
      writePackage(
        'object-factory',
        `
          class Mailer {
            constructor(prefix) { this.prefix = prefix; }
            async sendMail(message) {
              return { accepted: [message.to], subject: this.prefix + message.subject };
            }
          }
          export default {
            prefix: 'Welcome: ',
            createTransport(options) { return new Mailer(this.prefix + options.suffix); },
            nested: {
              base: 10,
              add(value) { return this.base + value; }
            }
          };
        `,
      ),
      writePackage(
        'class-default',
        `
          export default class Counter {
            constructor(start = 0) { this.value = start; }
            inc(step = 1) { this.value += step; return this; }
            getValue() { return this.value; }
            static label() { return 'counter'; }
          }
        `,
      ),
      writePackage(
        'named-exports',
        `
          export function double(value) { return value * 2; }
          export class Box {
            constructor(value) { this.value = value; }
            append(suffix) { return new Box(this.value + suffix); }
            unwrap() { return this.value; }
          }
          export const tools = {
            suffix: '!',
            shout(value) { return String(value).toUpperCase() + this.suffix; }
          };
        `,
      ),
      writePackage(
        'returns-builtins',
        `
          export default {
            getDateYear(date) { return date instanceof Date ? date.getUTCFullYear() : null; },
            makeDate() { return new Date('2020-01-02T00:00:00.000Z'); },
            makeMap() { return new Map([['a', 1]]); },
            makeSet() { return new Set(['x', 'y']); },
            makeArray() { return [1, 2, 3]; },
            makeString() { return 'hello world'; }
          };
        `,
      ),
      writePackage(
        'async-instance',
        `
          class Client {
            constructor(base) { this.base = base; }
            async request(path) { return { url: this.base + path }; }
          }
          export default async function createClient(base) {
            return new Client(base);
          }
        `,
      ),
      writePackage(
        'callback-runner',
        `
          class Box {
            constructor(value) { this.value = value; }
          }
          export class CallbackBox {
            constructor(value) { this.value = value; }
            async map(fn) { return new CallbackBox(await fn(this.value)); }
            unwrap() { return this.value; }
          }
          export default {
            makeBox(value) { return new Box(value); },
            readBox(box) { return box.value; },
            async call(value, fn) { return fn(value); },
            async nested(options) { return options.transform(options.value); }
          };
        `,
      ),
    ]);

    const service = new IsolatedExecutorService({
      packageCacheService: {
        getPackages: async () => packages.map((pkg) => pkg.name),
      } as any,
      packageCdnLoaderService: {
        getPackageSources: () => packages,
      } as any,
    });

    try {
      const result = await service.run(
        `
          const out = {};
          out.fn = await $ctx.$pkgs['fn-default'](' Hello Runtime ');

          const mailer = $ctx.$pkgs['object-factory'].createTransport({ suffix: 'User: ' });
          out.mail = await mailer.sendMail({ to: 'user@test.com', subject: 'Hello' });
          out.bound = await $ctx.$pkgs['object-factory'].nested.add(5);

          const Counter = $ctx.$pkgs['class-default'];
          out.staticLabel = await Counter.label();
          const counter = new Counter(4);
          out.counter = await counter.inc(3).inc(2).getValue();

          out.named = await $ctx.$pkgs['named-exports'].double(7);
          const box = new $ctx.$pkgs['named-exports'].Box('ok');
          out.box = await box.append('-mapped').unwrap();
          out.shout = await $ctx.$pkgs['named-exports'].tools.shout('hey');

          out.year = await $ctx.$pkgs['returns-builtins'].makeDate().getUTCFullYear();
          out.dateArgYear = await $ctx.$pkgs['returns-builtins'].getDateYear(new Date('2020-01-02T00:00:00.000Z'));
          out.mapValue = await $ctx.$pkgs['returns-builtins'].makeMap().get('a');
          out.setHas = await $ctx.$pkgs['returns-builtins'].makeSet().has('y');
          out.array = await $ctx.$pkgs['returns-builtins'].makeArray().map((value) => value * 2).join(',');
          out.string = await $ctx.$pkgs['returns-builtins'].makeString().toUpperCase().replaceAll(' ', '-');

          const client = $ctx.$pkgs['async-instance']('https://api.test');
          out.client = await client.request('/users');

          out.callback = await $ctx.$pkgs['callback-runner'].call('hello', async (value) => value + '!');
          out.nestedCallback = await $ctx.$pkgs['callback-runner'].nested({
            value: 'nested',
            transform: async (value) => value.toUpperCase()
          });
          const callbackBox = new $ctx.$pkgs['callback-runner'].CallbackBox('box');
          out.callbackBox = await callbackBox.map(async (value) => value + '-mapped').unwrap();
          const boxed = await $ctx.$pkgs['callback-runner'].makeBox('handle-arg');
          out.handleArg = await $ctx.$pkgs['callback-runner'].readBox(boxed);
          return out;
        `,
        createContext(),
        5000,
      );

      expect(result).toEqual({
        fn: 'hello-runtime',
        mail: {
          accepted: ['user@test.com'],
          subject: 'Welcome: User: Hello',
        },
        bound: 15,
        staticLabel: 'counter',
        counter: 9,
        named: 14,
        box: 'ok-mapped',
        shout: 'HEY!',
        year: 2020,
        dateArgYear: 2020,
        mapValue: 1,
        setHas: true,
        array: '2,4,6',
        string: 'HELLO-WORLD',
        client: { url: 'https://api.test/users' },
        callback: 'hello!',
        nestedCallback: 'NESTED',
        callbackBox: 'box-mapped',
        handleArg: 'handle-arg',
      });
    } finally {
      service.onDestroy();
    }
  });
});
