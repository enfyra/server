import { describe, expect, it } from 'vitest';
import { PackageCdnLoaderService } from '../../src/engines/cache';

describe('Package version path traversal protection', () => {
  function getArtifactDir(version: string): string {
    const loader = new PackageCdnLoaderService();
    return (loader as any).getPackageArtifactDir('test-pkg', version);
  }

  it('accepts normal semver version', () => {
    const dir = getArtifactDir('1.2.3');
    expect(dir).toContain('test_pkg@1.2.3');
  });

  it('accepts "latest" version', () => {
    const dir = getArtifactDir('latest');
    expect(dir).toContain('test_pkg@latest');
  });

  it('accepts prerelease version', () => {
    const dir = getArtifactDir('1.0.0-beta.1');
    expect(dir).toContain('test_pkg@1.0.0-beta.1');
  });

  it('rejects path traversal with ../', () => {
    expect(() => getArtifactDir('../../etc')).toThrow('Invalid package version');
  });

  it('rejects path traversal with ..\\', () => {
    expect(() => getArtifactDir('..\\..\\windows')).toThrow('Invalid package version');
  });

  it('rejects version with forward slash', () => {
    expect(() => getArtifactDir('1.0.0/../../etc/passwd')).toThrow('Invalid package version');
  });

  it('rejects version with null byte', () => {
    expect(() => getArtifactDir('1.0.0\x00')).toThrow('Invalid package version');
  });

  it('rejects version with spaces', () => {
    expect(() => getArtifactDir('1.0.0 evil')).toThrow('Invalid package version');
  });

  it('resolved path stays within cache directory', () => {
    const dir = getArtifactDir('2.0.0');
    const os = require('os');
    const path = require('path');
    const cacheDir = path.join(os.tmpdir(), 'enfyra-pkg-cache');
    expect(dir.startsWith(cacheDir + path.sep)).toBe(true);
  });
});
