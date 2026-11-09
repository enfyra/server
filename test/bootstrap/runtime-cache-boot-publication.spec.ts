import { expect, it, vi } from 'vitest';
import { init } from '../../src/init';
import { phaseParallelCacheReload } from '../../src/wiring/init/phases';

it('waits for started boot builders before releasing a failed publication', async () => {
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  let finished = false;
  let settled = false;
  const cradle = new Proxy({}, { get: (_target, name) => ({
    reload: name === 'routeCacheService'
      ? async () => { throw new Error('route build failed'); }
      : async () => { await blocked; finished = true; },
    installExtensions: async () => {},
  }) });
  const result = phaseParallelCacheReload(cradle as any).then(
    () => { settled = true; },
    error => { settled = true; throw error; },
  );
  const rejection = expect(result).rejects.toThrow('route build failed');
  await new Promise(resolve => setImmediate(resolve));
  const settledBeforeBuilders = settled;
  release();
  await rejection;
  expect(finished).toBe(true);
  expect(settledBeforeBuilders).toBe(false);
});

it('keeps all boot cache producers inside one publication through activation', async () => {
  let publishing = false;
  let commits = 0;
  const shared = { runWithPublication: async (work: () => Promise<void>) => { publishing = true; try { await work(); } finally { publishing = false; } }, commitPublication: async () => { commits++; } };
  const cache = () => ({ reload: vi.fn(async () => { expect(publishing).toBe(true); }), getCacheAsync: async () => [] });
  const cradle: any = new Proxy({}, { get(target: any, name: string) { return target[name] ??= new Proxy({}, { get: () => async () => {} }); } });
  Object.assign(cradle, { databaseConfigService: { getDbType: () => 'mongodb' }, firstRunInitializer: { isNeeded: async () => false }, eventEmitter: { emit: () => {} }, redisRuntimeCacheStore: shared, runtimeRegistryService: { init: async () => {}, publishFromCache: async () => {}, stageSnapshotFromCache: async () => { expect(publishing).toBe(true); return {}; }, activateSnapshots: () => { expect(commits).toBe(1); } } });
  for (const name of ['metadataCacheService', 'sharedHelperCacheService', 'routeCacheService', 'fieldPermissionCacheBuilder', 'columnRuleCacheBuilder', 'settingCacheService', 'storageConfigCacheBuilder', 'oauthConfigCacheBuilder', 'corsOriginCacheBuilder', 'websocketCacheBuilder', 'flowCacheBuilder', 'packageCacheService', 'folderTreeCacheService', 'guardCacheBuilder', 'gqlDefinitionCacheService']) cradle[name] = cache();
  await init({ cradle } as any);
  expect(commits).toBe(1);
});
