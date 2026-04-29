import { Logger } from '../../../shared/logger';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { pathToFileURL } from 'url';
import {
  applyDependencyHintToCdnSpecifier,
  getCdnDependencyFilePath,
  injectNodeEsmGlobals,
  isNativeCdnImport,
  NATIVE_CDN_STUB_SOURCE,
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
  private readonly dependencyManifestCache = new Map<
    string,
    Record<string, string>
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
    const results: PackageRuntimeDescriptor[] = [];
    for (const name of names) {
      try {
        const packageDir = this.findLatestPackageArtifactDir(name);
        const descriptor = packageDir
          ? this.getDescriptorFromArtifactDir(packageDir, name)
          : null;
        if (descriptor) results.push(descriptor);
      } catch {}
    }
    return results;
  }

  invalidateAll(): void {
    try {
      const files = fs.readdirSync(CACHE_DIR);
      for (const file of files) {
        fs.rmSync(path.join(CACHE_DIR, file), { recursive: true, force: true });
      }
    } catch {}
  }

  private getPackageArtifactDir(name: string, version: string): string {
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
    return path.join(CACHE_DIR, `${safeName}@${version}`);
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
  ): PackageRuntimeDescriptor | null {
    const filePath = path.join(packageDir, MAIN_FILE);
    if (!fs.existsSync(filePath)) return null;

    let manifest: { name?: string; version?: string } = {};
    try {
      manifest = JSON.parse(
        fs.readFileSync(path.join(packageDir, MANIFEST_FILE), 'utf-8'),
      );
    } catch {}

    const stat = fs.statSync(filePath);
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
    const tempDir = `${packageDir}.tmp-${process.pid}-${Date.now()}`;

    this.logger.log(`Fetching from CDN: ${spec} (${target})`);

    fs.rmSync(tempDir, { recursive: true, force: true });
    fs.mkdirSync(path.join(tempDir, DEPS_DIR), { recursive: true });

    const dependencyHints: CdnDependencyHints = new Map();
    const code = await this.fetchAndPrepareCdnModule(
      entryPath,
      name,
      version,
      tempDir,
      tempDir,
      new Set<string>(),
      new Map<string, Promise<string>>(),
      dependencyHints,
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

    fs.rmSync(packageDir, { recursive: true, force: true });
    fs.renameSync(tempDir, packageDir);
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
      return fs.readFileSync(new URL(specifier), 'utf-8');
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
  ): Promise<string> {
    const importPattern =
      /\b(from\s*["']|import\s*["'])(\/[^"']+|file:\/\/[^"']+|\.{1,2}\/[^"']+)(["'])/g;
    const specifiers = new Set<string>();
    for (const match of code.matchAll(importPattern)) {
      specifiers.add(match[2]);
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
        );
      }),
    );

    const replacements = new Map<string, string>();
    await Promise.all(
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
        if (isNativeCdnImport(resolvedSpecifier)) {
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
          );
          fs.writeFileSync(depPath, depCode, 'utf-8');
        }
        replacements.set(specifier, toRelativeImport(depPath, currentDir));
      }),
    );

    return code.replace(importPattern, (full, prefix, specifier, suffix) => {
      const replacement = replacements.get(specifier);
      return replacement ? `${prefix}${replacement}${suffix}` : full;
    });
  }

  private async collectDependencyHintsForSpecifier(
    specifier: string,
    dependencyHints: CdnDependencyHints,
  ): Promise<void> {
    const parsed = parseCdnPackageSpecifier(specifier);
    if (!parsed?.version) return;
    const cacheKey = `${parsed.name}@${parsed.version}`;
    let dependencies = this.dependencyManifestCache.get(cacheKey);
    if (!dependencies) {
      try {
        const manifest = JSON.parse(
          await this.fetchCdnPath(`/${cacheKey}/package.json`),
        ) as { dependencies?: Record<string, string> };
        dependencies = manifest.dependencies ?? {};
      } catch {
        dependencies = {};
      }
      this.dependencyManifestCache.set(cacheKey, dependencies);
    }

    for (const [dependencyName, dependencyVersion] of Object.entries(
      dependencies,
    )) {
      if (!dependencyHints.has(dependencyName)) {
        dependencyHints.set(dependencyName, dependencyVersion);
      }
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
