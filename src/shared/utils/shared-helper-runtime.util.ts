import type { SharedHelperDefinition } from '../types/shared-helper.types';
import { assertSharedHelperName } from './shared-helper-name.util';

const SCRIPT_MAX_BYTES = 4 * 1024 * 1024;
const RUNTIME_KEY = 'enfyra.shared-helper-runtime';

export function buildSharedHelperRuntime(
  definitions: readonly SharedHelperDefinition[],
): string {
  if (definitions.length > 1024)
    throw new RangeError('Shared helper definition limit exceeded');
  const names = new Set<string>();
  const entries = definitions.map((helper) => {
    assertSharedHelperName(helper.name);
    if (names.has(helper.name))
      throw new Error(`Duplicate helper name: ${helper.name}`);
    names.add(helper.name);
    if (
      typeof helper.isEnabled !== 'boolean' ||
      (helper.isEnabled && !helper.code?.trim())
    )
      throw new TypeError(`Invalid shared helper definition: ${helper.name}`);
    if (Buffer.byteLength(helper.sourceCode ?? '', 'utf8') > SCRIPT_MAX_BYTES)
      throw new RangeError(
        `Shared helper source limit exceeded: ${helper.name}`,
      );
    return `[${JSON.stringify(helper.name)}, { enabled: ${helper.isEnabled}, execute: ${helper.isEnabled ? `async function helper_${helper.name}($ctx) {\n${helper.code}\n}` : 'null'} }]`;
  });
  const source = `((definitions) => {
  const capabilities = $ctx;
  let calls = 0;
  const helpersFor = depth => new Proxy(capabilities.$helpers, {
    get: (target, key) => key === 'run' ? runner(depth) : Reflect.get(target, key),
  });
  const runner = depth => async (name, params) => {
    if (typeof name !== 'string') throw new TypeError('Helper name must be a string');
    const helper = definitions.get(name);
    if (!helper) throw new Error('Helper not found: ' + name);
    if (!helper.enabled) throw new Error('Helper is disabled: ' + name);
    if (depth >= 32) throw new Error('Helper call depth limit exceeded: ' + name);
    if (++calls > 1024) throw new Error('Helper call limit exceeded: ' + name);
    const context = Object.assign(Object.create(null), {
      $params: params,
      $body: {}, $query: {}, $share: {}, $user: null,
      $repos: capabilities.$repos, $transaction: capabilities.$transaction,
      $helpers: helpersFor(depth + 1), $cache: capabilities.$cache,
      $storage: capabilities.$storage, $socket: capabilities.$socket,
      $pkgs: capabilities.$pkgs, $streams: capabilities.$streams,
      $logs: capabilities.$logs, $throw: capabilities.$throw,
      $trigger: capabilities.$trigger, $res: capabilities.$res,
      $env: {},
    });
    try { return await helper.execute(context); }
    catch (error) {
      let carrier;
      try { carrier = JSON.parse(error?.message); } catch {}
      if (carrier?.__userThrow) throw error;
      throw new Error('Helper "' + name + '" failed: ' + (error?.message ?? String(error)));
    }
  };
  return { bind: context => new Proxy(context, {
    get: (target, key) => key === '$helpers' ? helpersFor(0) : Reflect.get(target, key),
  }) };
})(new Map([${entries.join(',\n')}]))`;
  if (Buffer.byteLength(source, 'utf8') > SCRIPT_MAX_BYTES - 1024)
    throw new RangeError(
      'Shared helper runtime exceeds the executor script limit',
    );
  return source;
}

export function wrapWithSharedHelpers(
  code: string,
  runtime: string,
  initialize: boolean,
): string {
  const key = `Symbol.for(${JSON.stringify(RUNTIME_KEY)})`;
  const binding = initialize
    ? `(globalThis[${key}] = ${runtime})`
    : `globalThis[${key}]`;
  const result = `return await (async function($ctx) {${code}\n})(${binding}.bind($ctx));`;
  if (Buffer.byteLength(result, 'utf8') > SCRIPT_MAX_BYTES - 256)
    throw new RangeError(
      'Composed script and helpers exceed the executor script limit',
    );
  return result;
}
