import { Logger } from '../../../shared/logger';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { pathToFileURL, fileURLToPath } from 'url';
import {
  applyDependencyHintToCdnSpecifier,
  getCdnDependencyFilePath,
  injectNodeEsmGlobals,
  isNativeCdnImport,
  NATIVE_CDN_STUB_SOURCE,
  OPTIONAL_PEER_CDN_STUB_SOURCE,
  parseCdnPackageSpecifier,
  resolveCdnImportSpecifier,
  suppressMissingModuleConsoleErrors,
  toRelativeImport,
  type CdnDependencyHints,
} from '../utils/package-cdn-loader.util';
import type { PackageRuntimeDescriptor } from '../../../shared/types';

const CDN_BASE = 'https://esm.sh';
const CACHE_DIR = path.join(os.tmpdir(), 'enfyra-pkg-cache');
const MAIN_FILE = 'main.mjs';
const DEPS_DIR = 'deps';
const MANIFEST_FILE = 'manifest.json';
const CDN_FETCH_TIMEOUT_MS = 20_000;
type CdnBundleTarget = 'node' | 'es2022';
type CdnPreparedModules = Map<string, Promise<string>>;
type CdnPackageDependencyManifest = {
  dependencies: Record<string, string>;
  optionalPeerDependencies: string[];
};

export function extractErrorMessage(error: any): string {
  const parts: string[] = [];
  let current = error;
  while (current) {
    if (current.message) parts.push(current.message);
    current = current.cause;
  }
  return parts.join(' → ') || 'Unknown error';
}

export class PackageCdnLoaderService {
  private readonly logger = new Logger(PackageCdnLoaderService.name);
  private readonly packageLoads = new Map<
    string,
    Promise<PackageRuntimeDescriptor>
  >();
  private readonly dependencyManifestCache = new Map<
    string,
    CdnPackageDependencyManifest
  >();
  private readonly artifactManifests = new Map<
    string,
    { stamp: string; manifest: { name?: string; version?: string } }
  >();

  constructor() {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
  }

  getLoadedPackages(): Map<string, PackageRuntimeDescriptor> {
    const loaded = new Map<string, PackageRuntimeDescriptor>();
    try {
      const files = fs.readdirSync(CACHE_DIR);
      for (const file of files) {
        const packageDir = path.join(CACHE_DIR, file);
        const descriptor = this.getDescriptorFromArtifactDir(packageDir);
        if (descriptor)
          loaded.set(`${descriptor.name}@${descriptor.version}`, descriptor);
      }
    } catch {}
    return loaded;
  }

  isLoaded(name: string, version?: string): boolean {
    if (version) return fs.existsSync(this.getMainFilePath(name, version));
    return this.findLatestPackageArtifactDir(name) !== null;
  }

  getModule(name: string): PackageRuntimeDescriptor | undefined {
    return this.getPackageSources([name])[0];
  }

  async loadPackage(
    name: string,
    version: string,
  ): Promise<PackageRuntimeDescriptor> {
    const filePath = this.getMainFilePath(name, version);
    const pending = this.packageLoads.get(filePath);
    if (pending) return pending;

    const loading = (async () => {
      if (!fs.existsSync(filePath)) {
        await this.fetchAndWriteBundle(name, version, 'node');
      }
      const descriptor = this.getPackageDescriptor(name, version);
      if (!descriptor) {
        throw new Error(
          `Package artifact missing after CDN load: ${name}@${version}`,
        );
      }
      return descriptor;
    })();
    this.packageLoads.set(filePath, loading);
    try {
      return await loading;
    } finally {
      this.packageLoads.delete(filePath);
    }
  }

  async preloadPackages(
    packages: Array<{ name: string; version: string }>,
  ): Promise<void> {
    for (const pkg of packages) {
      try {
        await this.loadPackage(pkg.name, pkg.version);
        this.logger.log(`Preloaded: ${pkg.name}@${pkg.version}`);
      } catch (error) {
        this.logger.error(
          `Failed to preload ${pkg.name}@${pkg.version}: ${extractErrorMessage(error)}`,
        );
      }
    }
  }

  async invalidatePackage(name: string, newVersion?: string): Promise<void> {
    this.deletePackageArtifacts(name);

    if (newVersion) {
      try {
        await this.loadPackage(name, newVersion);
      } catch (error) {
        this.logger.error(
          `Failed to reload ${name}@${newVersion}: ${extractErrorMessage(error)}`,
        );
      }
    }
  }

  getPackageSources(names: string[]): PackageRuntimeDescriptor[] {
    if (names.length === 0) return [];
    const prefixes = new Set(names.map(name => `${name.replace(/[^a-zA-Z0-9]/g, '_')}@`));
    const selected = new Map<string, { directory: string; stat: fs.Stats }>();
    try {
      for (const file of fs.readdirSync(CACHE_DIR)) {
        const prefix = file.slice(0, file.lastIndexOf('@') + 1);
        if (!prefixes.has(prefix)) continue;
        try {
          const directory = path.join(CACHE_DIR, file);
          const stat = fs.statSync(path.join(directory, MAIN_FILE));
          const current = selected.get(prefix);
          if (!current || stat.mtimeMs > current.stat.mtimeMs) {
            selected.set(prefix, { directory, stat });
          }
        } catch {}
      }
    } catch {
      return [];
    }
    const results: PackageRuntimeDescriptor[] = [];
    for (const name of names) {
      try {
        const artifact = selected.get(`${name.replace(/[^a-zA-Z0-9]/g, '_')}@`);
        const descriptor = artifact
          ? this.getDescriptorFromArtifactDir(artifact.directory, name, undefined, artifact.stat)
          : null;
        if (descriptor) results.push(descriptor);
      } catch {}
    }
    return results;
  }

  invalidateAll(): void {
    this.artifactManifests.clear();
    try {
      const files = fs.readdirSync(CACHE_DIR);
      for (const file of files) {
        fs.rmSync(path.join(CACHE_DIR, file), { recursive: true, force: true });
      }
    } catch {}
  }

  private getPackageArtifactDir(name: string, version: string): string {
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
    if (!/^[a-zA-Z0-9._-]+$/.test(version)) {
      throw new Error(`Invalid package version: ${version}`);
    }
    const dir = path.resolve(CACHE_DIR, `${safeName}@${version}`);
    if (!dir.startsWith(CACHE_DIR + path.sep)) {
      throw new Error(`Package path escapes cache directory`);
    }
    return dir;
  }

  private getMainFilePath(name: string, version: string): string {
    return path.join(this.getPackageArtifactDir(name, version), MAIN_FILE);
  }

  private getPackageDescriptor(
    name: string,
    version: string,
  ): PackageRuntimeDescriptor | null {
    return this.getDescriptorFromArtifactDir(
      this.getPackageArtifactDir(name, version),
      name,
      version,
    );
  }

  private getDescriptorFromArtifactDir(
    packageDir: string,
    fallbackName?: string,
    fallbackVersion?: string,
    fileStat?: fs.Stats,
  ): PackageRuntimeDescriptor | null {
    const filePath = path.join(packageDir, MAIN_FILE);
    if (!fileStat && !fs.existsSync(filePath)) return null;

    let manifest: { name?: string; version?: string } = {};
    try {
      const manifestPath = path.join(packageDir, MANIFEST_FILE);
      const metadata = fs.statSync(manifestPath, { bigint: true });
      const stamp = `${metadata.dev}:${metadata.ino}:${metadata.size}:${metadata.mtimeNs}:${metadata.ctimeNs}`;
      const cached = this.artifactManifests.get(packageDir);
      if (cached?.stamp === stamp) {
        manifest = cached.manifest;
      } else {
        manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8'));
        this.artifactManifests.delete(packageDir);
        if (metadata.size <= 64n * 1024n) {
          this.artifactManifests.set(packageDir, { stamp, manifest });
        }
        while (this.artifactManifests.size > 500) {
          const oldest = this.artifactManifests.keys().next().value;
          if (oldest === undefined) break;
          this.artifactManifests.delete(oldest);
        }
      }
    } catch {
      this.artifactManifests.delete(packageDir);
    }

    const stat = fileStat ?? fs.statSync(filePath);
    const name =
      manifest.name ??
      fallbackName ??
      path.basename(packageDir).replace(/@[^@]+$/, '');
    const version =
      manifest.version ??
      fallbackVersion ??
      path.basename(packageDir).split('@').slice(1).join('@') ??
      'latest';
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');

    return {
      name,
      safeName,
      version,
      filePath,
      fileUrl: pathToFileURL(filePath).href,
      size: stat.size,
      mtimeMs: stat.mtimeMs,
      cacheKey: `${name}@${version}:${stat.mtimeMs}:${stat.size}`,
    };
  }

  private getDepsDir(name: string, version: string): string {
    return path.join(this.getPackageArtifactDir(name, version), DEPS_DIR);
  }

  private findLatestPackageArtifactDir(name: string): string | null {
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
    const prefix = `${safeName}@`;
    const files = fs.readdirSync(CACHE_DIR);
    const match = files
      .filter((file) => {
        const full = path.join(CACHE_DIR, file);
        return (
          file.startsWith(prefix) && fs.existsSync(path.join(full, MAIN_FILE))
        );
      })
      .sort((a, b) => {
        const aStat = fs.statSync(path.join(CACHE_DIR, a, MAIN_FILE));
        const bStat = fs.statSync(path.join(CACHE_DIR, b, MAIN_FILE));
        return bStat.mtimeMs - aStat.mtimeMs;
      })[0];
    return match ? path.join(CACHE_DIR, match) : null;
  }

  private deletePackageArtifacts(name: string): void {
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
    const prefix = `${safeName}@`;
    for (const directory of this.artifactManifests.keys()) {
      if (path.basename(directory).startsWith(prefix)) this.artifactManifests.delete(directory);
    }
    try {
      const files = fs.readdirSync(CACHE_DIR);
      for (const file of files) {
        if (!file.startsWith(prefix)) continue;
        fs.rmSync(path.join(CACHE_DIR, file), {
          recursive: true,
          force: true,
        });
      }
    } catch {}
  }

  private async fetchAndWriteBundle(
    name: string,
    version: string,
    target: CdnBundleTarget = 'node',
  ): Promise<void> {
    const spec = `${name}@${version}`;
    const entryPath = `/${spec}?bundle&target=${target}`;
    const packageDir = this.getPackageArtifactDir(name, version);
    const stagingDir = path.join(CACHE_DIR, '.staging');
    fs.mkdirSync(stagingDir, { recursive: true });
    const tempDir = fs.mkdtempSync(path.join(stagingDir, 'package-'));

    this.logger.log(`Fetching from CDN: ${spec} (${target})`);

    try {
      fs.mkdirSync(path.join(tempDir, DEPS_DIR), { recursive: true });
      const dependencyHints: CdnDependencyHints = new Map();
      const optionalPeerDependencies = new Set<string>();
      const code = await this.fetchAndPrepareCdnModule(
        entryPath,
        name,
        version,
        tempDir,
        tempDir,
        new Set<string>(),
        new Map<string, Promise<string>>(),
        dependencyHints,
        optionalPeerDependencies,
      );

      fs.writeFileSync(path.join(tempDir, MAIN_FILE), code, 'utf-8');
      fs.writeFileSync(
        path.join(tempDir, MANIFEST_FILE),
        JSON.stringify(
          {
            name,
            version,
            main: MAIN_FILE,
            deps: DEPS_DIR,
            source: CDN_BASE,
            target,
            installedAt: new Date().toISOString(),
          },
          null,
          2,
        ),
        'utf-8',
      );

      await this.publishArtifact(tempDir, packageDir);
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  }

  private async publishArtifact(tempDir: string, packageDir: string): Promise<void> {
    const lockDir = path.join(
      path.dirname(tempDir),
      `publish-${path.basename(packageDir).split('@')[0]}.lock`,
    );
    const ownerFile = path.join(lockDir, `owner-${process.pid}`);
    const deadline = performance.now() + 5000;
    while (true) {
      try {
        fs.mkdirSync(lockDir);
        try {
          fs.writeFileSync(ownerFile, '', { flag: 'wx' });
        } catch (error) {
          fs.rmdirSync(lockDir);
          throw error;
        }
        break;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
        if (this.reclaimPublicationLock(lockDir)) continue;
        if (performance.now() >= deadline) {
          throw new Error('Package artifact publication lock timed out; inspect its owner before recovery');
        }
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    }
    try {
      try {
        fs.renameSync(tempDir, packageDir);
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code !== 'ENOTEMPTY' && code !== 'EEXIST') throw error;
        if (fs.existsSync(path.join(packageDir, MAIN_FILE))) return;
        // All publishers hold this lock before inspecting or replacing the destination.
        fs.rmSync(packageDir, { recursive: true, force: true });
        fs.renameSync(tempDir, packageDir);
      }
    } finally {
      fs.unlinkSync(ownerFile);
      fs.rmdirSync(lockDir);
    }
  }

  private reclaimPublicationLock(lockDir: string): boolean {
    try {
      const owners = fs.readdirSync(lockDir);
      if (owners.length !== 1) return false;
      const owner = owners[0];
      if (!owner || !/^owner-[1-9]\d*$/.test(owner)) return false;
      const pid = Number(owner.slice(6));
      if (!Number.isSafeInteger(pid)) return false;
      try {
        process.kill(pid, 0);
        return false;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') return false;
      }
      fs.unlinkSync(path.join(lockDir, owner));
      fs.rmdirSync(lockDir);
      return true;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === 'ENOENT') return true;
      if (code === 'ENOTEMPTY' || code === 'EEXIST') return false;
      throw error;
    }
  }

  private async fetchAndPrepareCdnModule(
    specifier: string,
    name: string,
    version: string,
    packageDir = this.getPackageArtifactDir(name, version),
    currentDir = packageDir,
    activeStack = new Set<string>(),
    preparedModules: CdnPreparedModules = new Map(),
    dependencyHints: CdnDependencyHints = new Map(),
    optionalPeerDependencies = new Set<string>(),
  ): Promise<string> {
    specifier = applyDependencyHintToCdnSpecifier(specifier, dependencyHints);
    if (activeStack.has(specifier)) {
      const existingPath = getCdnDependencyFilePath(
        specifier,
        packageDir,
        DEPS_DIR,
      );
      if (fs.existsSync(existingPath)) {
        return fs.readFileSync(existingPath, 'utf-8');
      }
      return '';
    }
    const existingPrepare = preparedModules.get(specifier);
    if (existingPrepare) return existingPrepare;

    const prepare = (async () => {
      activeStack.add(specifier);
      try {
        let code = await this.loadModuleSource(specifier);
        await this.collectDependencyHintsForSpecifier(
          specifier,
          dependencyHints,
          optionalPeerDependencies,
        );
        code = suppressMissingModuleConsoleErrors(code);
        code = injectNodeEsmGlobals(code);
        code = await this.rewriteCdnSpecifiers(
          code,
          specifier,
          name,
          version,
          packageDir,
          currentDir,
          activeStack,
          preparedModules,
          dependencyHints,
          optionalPeerDependencies,
        );
        return code;
      } finally {
        activeStack.delete(specifier);
      }
    })();
    preparedModules.set(specifier, prepare);
    return prepare;
  }

  private async loadModuleSource(specifier: string): Promise<string> {
    if (specifier.startsWith('file://')) {
      const filePath = path.resolve(fileURLToPath(new URL(specifier)));
      if (!filePath.startsWith(CACHE_DIR + path.sep)) {
        throw new Error(
          `Refusing to load '${specifier}': package modules may only read from the package cache`,
        );
      }
      return fs.readFileSync(filePath, 'utf-8');
    }
    return this.fetchCdnPath(specifier);
  }

  private async fetchCdnPath(cdnPath: string): Promise<string> {
    let res: Response;
    const url = `${CDN_BASE}${cdnPath}`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), CDN_FETCH_TIMEOUT_MS);
    try {
      res = await fetch(url, { signal: controller.signal });
    } catch (error) {
      throw new Error(
        `CDN fetch failed for ${cdnPath}: ${extractErrorMessage(error)}`,
      );
    } finally {
      clearTimeout(timeout);
    }

    if (!res.ok) {
      throw new Error(
        `CDN fetch failed for ${cdnPath}: ${res.status} ${res.statusText}`,
      );
    }

    return this.withTimeout(
      res.text(),
      CDN_FETCH_TIMEOUT_MS,
      `CDN response read timed out for ${cdnPath}`,
    );
  }

  private async rewriteCdnSpecifiers(
    code: string,
    sourceSpecifier: string,
    name: string,
    version: string,
    packageDir: string,
    currentDir: string,
    activeStack: Set<string>,
    preparedModules: CdnPreparedModules,
    dependencyHints: CdnDependencyHints,
    optionalPeerDependencies: Set<string>,
  ): Promise<string> {
    const importPattern =
      /\b(from\s*["']|import\s*["'])(\/[^"']+|file:\/\/[^"']+|\.{1,2}\/[^"']+)(["'])/g;
    const specifiers = new Set<string>();
    for (const match of code.matchAll(importPattern)) {
      const specifier = match[2];
      if (specifier !== undefined) specifiers.add(specifier);
    }

    await Promise.all(
      [...specifiers].map((specifier) => {
        const resolvedSpecifier = resolveCdnImportSpecifier(
          specifier,
          sourceSpecifier,
        );
        return this.collectDependencyHintsForSpecifier(
          resolvedSpecifier,
          dependencyHints,
          optionalPeerDependencies,
        );
      }),
    );

    const replacements = new Map<string, string>();
    const results = await Promise.allSettled(
      [...specifiers].map(async (specifier) => {
        const resolvedSpecifier = applyDependencyHintToCdnSpecifier(
          resolveCdnImportSpecifier(specifier, sourceSpecifier),
          dependencyHints,
        );
        const depPath = getCdnDependencyFilePath(
          resolvedSpecifier,
          packageDir,
          DEPS_DIR,
        );
        const parsedDependency = parseCdnPackageSpecifier(resolvedSpecifier);
        if (
          parsedDependency &&
          optionalPeerDependencies.has(parsedDependency.name)
        ) {
          fs.writeFileSync(depPath, OPTIONAL_PEER_CDN_STUB_SOURCE, 'utf-8');
        } else if (isNativeCdnImport(resolvedSpecifier)) {
          fs.writeFileSync(depPath, NATIVE_CDN_STUB_SOURCE, 'utf-8');
        } else if (!fs.existsSync(depPath)) {
          const depCode = await this.fetchAndPrepareCdnModule(
            resolvedSpecifier,
            name,
            version,
            packageDir,
            path.dirname(depPath),
            new Set(activeStack),
            preparedModules,
            dependencyHints,
            optionalPeerDependencies,
          );
          fs.writeFileSync(depPath, depCode, 'utf-8');
        }
        replacements.set(specifier, toRelativeImport(depPath, currentDir));
      }),
    );
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;

    return code.replace(importPattern, (full, prefix, specifier, suffix) => {
      const replacement = replacements.get(specifier);
      return replacement ? `${prefix}${replacement}${suffix}` : full;
    });
  }

  private async collectDependencyHintsForSpecifier(
    specifier: string,
    dependencyHints: CdnDependencyHints,
    optionalPeerDependencies: Set<string>,
  ): Promise<void> {
    const parsed = parseCdnPackageSpecifier(specifier);
    if (!parsed?.version) return;
    const cacheKey = `${parsed.name}@${parsed.version}`;
    let dependencyManifest = this.dependencyManifestCache.get(cacheKey);
    if (!dependencyManifest) {
      try {
        const manifest = JSON.parse(
          await this.fetchCdnPath(`/${cacheKey}/package.json`),
        ) as {
          dependencies?: Record<string, string>;
          peerDependencies?: Record<string, string>;
          peerDependenciesMeta?: Record<string, { optional?: boolean }>;
        };
        dependencyManifest = {
          dependencies: manifest.dependencies ?? {},
          optionalPeerDependencies: Object.entries(
            manifest.peerDependenciesMeta ?? {},
          )
            .filter(
              ([peerName, metadata]) =>
                metadata.optional === true &&
                Boolean(manifest.peerDependencies?.[peerName]),
            )
            .map(([peerName]) => peerName),
        };
      } catch {
        dependencyManifest = {
          dependencies: {},
          optionalPeerDependencies: [],
        };
      }
      this.dependencyManifestCache.set(cacheKey, dependencyManifest);
    }

    for (const [dependencyName, dependencyVersion] of Object.entries(
      dependencyManifest.dependencies,
    )) {
      if (!dependencyHints.has(dependencyName)) {
        dependencyHints.set(dependencyName, dependencyVersion);
      }
    }
    for (const peerName of dependencyManifest.optionalPeerDependencies) {
      optionalPeerDependencies.add(peerName);
    }
  }

  private withTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    message: string,
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
      promise.then(
        (value) => {
          clearTimeout(timeout);
          resolve(value);
        },
        (error) => {
          clearTimeout(timeout);
          reject(error);
        },
      );
    });
  }
}
