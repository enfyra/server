import { afterEach, describe, expect, it } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { ExecutorEngineService, IsolatedExecutorService } from '@enfyra/kernel';
import { RuntimeScriptExecutorService } from '../../src/engines/cache/services/runtime-script-executor.service';
import { assertScriptSourceContract, compileScriptSource } from '../../src/shared/utils/script-code.util';

const instances: IsolatedExecutorService[] = [];
const directories: string[] = [];
afterEach(() => {
  for (const service of instances.splice(0)) service.onDestroy();
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});
function fixture() {
  const directory = mkdtempSync(join(tmpdir(), 'esv-admission-'));
  directories.push(directory);
  const filePath = join(directory, 'fixture.mjs');
  writeFileSync(filePath, 'export function echo(value) { return value; }');
  const isolated = new IsolatedExecutorService({
    packageCacheService: { getPackages: () => ['fixture'] },
    packageCdnLoaderService: { getPackageSources: () => [{ name: 'fixture', safeName: 'fixture', sourceCode: 'export function echo(value) { return value; }', filePath, fileUrl: pathToFileURL(filePath).href }] },
    tuning: { maxConcurrentWorkers: 1, isolatesPerWorker: 1, isolateMemoryLimitMb: 64 },
  });
  instances.push(isolated);
  const runtime = new RuntimeScriptExecutorService({ kernelExecutorEngineService: new ExecutorEngineService({ isolatedExecutorService: isolated }), runtimeRegistryService: { getSharedHelpers: () => [] } } as any);
  return async (sourceCode: string) => {
    assertScriptSourceContract(sourceCode);
    return runtime.run(compileScriptSource(sourceCode, 'typescript')!, { $body: {}, $share: {} }, 2000, { sourceCode, scriptLanguage: 'typescript' });
  };
}
describe('actual local ESV package admission boundaries', () => {
  it('blocks a Node module not supplied by package descriptors', async () => {
    const run = fixture();
    await expect(run('return require("node:fs");')).rejects.toThrow(/not available/);
  });
  it('observes the existing supplied-package require shim through ESV source compilation', async () => {
    const run = fixture();
    await expect(run('return require("fixture").echo(42);')).resolves.toBe(42);
  });
  it('executes the supported package macro through its proxy', async () => {
    const run = fixture();
    await expect(run('return await @PKGS.fixture.echo(42);')).resolves.toBe(42);
  });
  it('does not expose host process or require through globalThis', async () => {
    const run = fixture();
    await expect(run('return { process: typeof process, globalRequire: typeof globalThis.require };')).resolves.toEqual({ process: 'undefined', globalRequire: 'undefined' });
  });
});
