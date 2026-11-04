import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'node:child_process';

const tempRoot = vi.hoisted(() => {
  const fs = require('fs');
  const os = require('os');
  const path = require('path');
  return fs.mkdtempSync(path.join(os.tmpdir(), 'enfyra-descriptor-discovery-')) as string;
});
vi.mock('fs', async importOriginal => {
  const actual = await importOriginal<typeof import('fs')>();
  return { ...actual, readFileSync: vi.fn(actual.readFileSync), readdirSync: vi.fn(actual.readdirSync) };
});
vi.mock('os', async importOriginal => ({
  ...(await importOriginal<typeof import('os')>()),
  tmpdir: () => tempRoot,
}));
import { PackageCdnLoaderService } from '../../src/engines/cache/services/package-cdn-loader.service';

const cacheDir = path.join(tempRoot, 'enfyra-pkg-cache');
function artifact(name: string, version: string, modified: number, source = 'export default 42;') {
  const directory = path.join(cacheDir, `${name.replace(/[^a-zA-Z0-9]/g, '_')}@${version}`);
  fs.mkdirSync(directory, { recursive: true });
  const main = path.join(directory, 'main.mjs');
  fs.writeFileSync(main, source);
  fs.writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify({ name, version }));
  fs.utimesSync(main, modified, modified);
  return { directory, main, manifest: path.join(directory, 'manifest.json') };
}

beforeEach(() => { fs.mkdirSync(cacheDir, { recursive: true }); });
afterEach(() => { vi.restoreAllMocks(); fs.rmSync(cacheDir, { recursive: true, force: true }); });
afterAll(() => { fs.rmSync(tempRoot, { recursive: true, force: true }); });

describe('disk-first package descriptor discovery', () => {
  it('scans the directory once per list and avoids rereading unchanged manifests', () => {
    artifact('one', '1', 100);
    artifact('two', '1', 100);
    const loader = new PackageCdnLoaderService();
    loader.getPackageSources(['one', 'two']);
    const read = vi.mocked(fs.readFileSync).mockClear();
    const scan = vi.mocked(fs.readdirSync).mockClear();
    expect(loader.getPackageSources(['one', 'two']).map(pkg => pkg.name)).toEqual(['one', 'two']);
    expect(scan).toHaveBeenCalledTimes(1);
    expect(read).not.toHaveBeenCalled();
  });
  it('bounds manifest retention and does not cache oversized manifests', () => {
    const files = artifact('oversized', '1', 100);
    fs.writeFileSync(files.manifest, JSON.stringify({ name: 'oversized', version: '1', extra: 'x'.repeat(64 * 1024) }));
    const loader = new PackageCdnLoaderService();
    loader.getModule('oversized');
    const read = vi.mocked(fs.readFileSync).mockClear();
    expect(loader.getModule('oversized')?.version).toBe('1');
    expect(read).toHaveBeenCalledTimes(1);
  });
  it('performs no filesystem work for an empty descriptor list', () => {
    const loader = new PackageCdnLoaderService();
    const scan = vi.spyOn(fs, 'readdirSync');
    expect(loader.getPackageSources([])).toEqual([]);
    expect(scan).not.toHaveBeenCalled();
  });
  it('keeps latest-by-mtime selection, caller order and duplicates', () => {
    artifact('one', '2', 100);
    artifact('one', '1', 200);
    artifact('two', '1', 100);
    const loader = new PackageCdnLoaderService();
    expect(loader.getPackageSources(['two', 'missing', 'one', 'one']).map(pkg => [pkg.name, pkg.version])).toEqual([['two', '1'], ['one', '1'], ['one', '1']]);
  });
  it('observes peer publication and removal without invalidation or a TTL', async () => {
    artifact('peer', '1', 100);
    const loader = new PackageCdnLoaderService();
    expect(loader.getModule('peer')?.version).toBe('1');
    const latest = artifact('peer', '2', 200);
    const peer = new PackageCdnLoaderService();
    await peer.loadPackage('peer', '2');
    expect(loader.getModule('peer')?.version).toBe('2');
    fs.unlinkSync(latest.main);
    expect(loader.getModule('peer')?.version).toBe('1');
    await peer.invalidatePackage('peer');
    expect(loader.getModule('peer')).toBeUndefined();
  });
  it('observes publication from an independent Node process', () => {
    artifact('process-peer', '1', 100);
    const loader = new PackageCdnLoaderService();
    expect(loader.getModule('process-peer')?.version).toBe('1');
    const directory = path.join(cacheDir, 'process_peer@2');
    execFileSync(process.execPath, ['-e', `const fs=require('node:fs');const path=require('node:path');const directory=process.argv[1];fs.mkdirSync(directory);const main=path.join(directory,'main.mjs');fs.writeFileSync(main,'export default 2;');fs.utimesSync(main,200,200);fs.writeFileSync(path.join(directory,'manifest.json'),JSON.stringify({name:'process-peer',version:'2'}));`, directory], { timeout: 5000 });
    expect(loader.getModule('process-peer')?.version).toBe('2');
  });
  it('detects same-sized manifest replacement even when its mtime is restored', () => {
    const files = artifact('stamp', '1', 100);
    const loader = new PackageCdnLoaderService();
    expect(loader.getModule('stamp')?.version).toBe('1');
    const previous = fs.statSync(files.manifest);
    const replacement = `${files.manifest}.next`;
    fs.writeFileSync(replacement, JSON.stringify({ name: 'stamp', version: '2' }));
    fs.utimesSync(replacement, previous.atime, previous.mtime);
    fs.renameSync(replacement, files.manifest);
    expect(loader.getModule('stamp')?.version).toBe('2');
  });
  it('refreshes descriptor size, mtime and manifest-only edits immediately', () => {
    const files = artifact('edit', '1', 100);
    const loader = new PackageCdnLoaderService();
    const first = loader.getModule('edit')!;
    fs.writeFileSync(files.main, 'export default "longer source";');
    fs.utimesSync(files.main, 200, 200);
    expect(loader.getModule('edit')).toMatchObject({ size: fs.statSync(files.main).size, mtimeMs: 200000 });
    fs.writeFileSync(files.manifest, JSON.stringify({ name: 'edited-manifest-name', version: 'manifest-version' }));
    expect(loader.getModule('edit')).toMatchObject({ name: 'edited-manifest-name', version: 'manifest-version' });
    expect(first.version).toBe('1');
  });
  it('does not let caller mutations poison later descriptors', () => {
    artifact('mutable', '1', 100);
    const loader = new PackageCdnLoaderService();
    const first = loader.getModule('mutable')!;
    first.name = 'corrupt';
    first.filePath = '/not-a-package';
    expect(loader.getModule('mutable')).toMatchObject({ name: 'mutable', version: '1' });
    expect(loader.getModule('mutable')?.filePath).not.toBe(first.filePath);
  });
  it('keeps missing/malformed manifest fallbacks and recovers without negative caching', () => {
    const files = artifact('fallback', '1', 100);
    fs.unlinkSync(files.manifest);
    const loader = new PackageCdnLoaderService();
    expect(loader.getModule('fallback')).toMatchObject({ name: 'fallback', version: '1' });
    fs.writeFileSync(files.manifest, 'invalid JSON');
    expect(loader.getModule('fallback')).toMatchObject({ name: 'fallback', version: '1' });
    fs.writeFileSync(files.manifest, JSON.stringify({ name: 'fallback', version: 'repaired' }));
    expect(loader.getModule('fallback')?.version).toBe('repaired');
    fs.unlinkSync(files.main);
    expect(loader.getModule('fallback')).toBeUndefined();
    fs.writeFileSync(files.main, 'export default 7;');
    expect(loader.getModule('fallback')?.version).toBe('repaired');
  });
  it('supports same-loader invalidateAll and subsequent artifact creation', () => {
    artifact('cleared', '1', 100);
    const loader = new PackageCdnLoaderService();
    expect(loader.getModule('cleared')).toBeDefined();
    loader.invalidateAll();
    expect(loader.getModule('cleared')).toBeUndefined();
    artifact('cleared', '2', 200);
    expect(loader.getModule('cleared')?.version).toBe('2');
  });
  it('preserves best-effort missing-directory behavior and ignores unpublished staging', () => {
    const loader = new PackageCdnLoaderService();
    fs.mkdirSync(path.join(cacheDir, '.staging', 'candidate'), { recursive: true });
    fs.writeFileSync(path.join(cacheDir, '.staging', 'candidate', 'main.mjs'), 'export default 1');
    expect(loader.getPackageSources(['candidate'])).toEqual([]);
    fs.rmSync(cacheDir, { recursive: true });
    expect(loader.getPackageSources(['candidate'])).toEqual([]);
  });
});
