import { Logger } from '../../../shared/logger';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { pathToFileURL } from 'url';
import { createHash } from 'crypto';

const CDN_BASE = 'https://esm.sh';
const CACHE_DIR = path.join(os.tmpdir(), 'enfyra-pkg-cache');
const MAIN_FILE = 'main.mjs';
const DEPS_DIR = 'deps';
const MANIFEST_FILE = 'manifest.json';
const CDN_FETCH_TIMEOUT_MS = 20_000;
const CDN_IMPORT_TIMEOUT_MS = 10_000;
const NATIVE_CDN_STUB_SOURCE =
  'export function getCPUInfo() { return {}; }\nexport default { getCPUInfo };\n';
type CdnBundleTarget = 'node' | 'es2022';
type CdnDependencyHints = Map<string, string>;
type ParsedCdnPackageSpecifier = {
  name: string;
  version: string | null;
  packagePath: string;
  query: string;
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

const WORKER_RUNTIME_ERROR_PATTERNS = [
  'Cannot find module',
  'require is not defined',
  'process is not defined',
  'Unresolved import',
  '.node',
  'node-gyp',
  'bindings',
  'dynamic import callback',
];

export class PackageCdnLoaderService {
  private readonly logger = new Logger(PackageCdnLoaderService.name);
  private readonly moduleCache = new Map<string, any>();
  private readonly dependencyManifestCache = new Map<
    string,
    Record<string, string>
  >();

  constructor() {
    if (!fs.existsSync(CACHE_DIR)) {
      fs.mkdirSync(CACHE_DIR, { recursive: true });
    }
  }

  getLoadedPackages(): Map<string, any> {
    return this.moduleCache;
  }

  isLoaded(name: string): boolean {
    for (const key of this.moduleCache.keys()) {
      if (key === name || key.startsWith(`${name}@`)) return true;
    }
    return false;
  }

  getModule(name: string): any | undefined {
    for (const [key, mod] of this.moduleCache) {
      if (key === name || key.startsWith(`${name}@`)) return mod;
    }
    return undefined;
  }

  async loadPackage(name: string, version: string): Promise<any> {
    const cacheKey = `${name}@${version}`;

    if (this.moduleCache.has(cacheKey)) {
      return this.moduleCache.get(cacheKey);
    }

    const filePath = this.getMainFilePath(name, version);

    if (!fs.existsSync(filePath)) {
      await this.fetchAndWriteBundle(name, version, 'node');
    }

    let mod: any;
    try {
      mod = await this.importFromFile(filePath, name);
    } catch (error) {
      mod = await this.refetchAndImportPackage(name, version, filePath, error);
    }
    this.moduleCache.set(cacheKey, mod);
    return mod;
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
    for (const key of this.moduleCache.keys()) {
      if (key === name || key.startsWith(`${name}@`)) {
        this.moduleCache.delete(key);
      }
    }

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

  getPackageSources(names: string[]): Array<{
    name: string;
    safeName: string;
    sourceCode: string;
    filePath: string;
    fileUrl: string;
  }> {
    const results: Array<{
      name: string;
      safeName: string;
      sourceCode: string;
      filePath: string;
      fileUrl: string;
    }> = [];
    for (const name of names) {
      const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
      try {
        const packageDir = this.findLatestPackageArtifactDir(name);
        if (packageDir) {
          const filePath = path.join(packageDir, MAIN_FILE);
          if (!fs.existsSync(filePath)) continue;
          const sourceCode = fs.readFileSync(filePath, 'utf-8');
          results.push({
            name,
            safeName,
            sourceCode,
            filePath,
            fileUrl: pathToFileURL(filePath).href,
          });
        }
      } catch {}
    }
    return results;
  }

  invalidateAll(): void {
    this.moduleCache.clear();
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

  private getDepsDir(name: string, version: string): string {
    return path.join(this.getPackageArtifactDir(name, version), DEPS_DIR);
  }

  private findLatestPackageArtifactDir(name: string): string | null {
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
    const prefix = `${safeName}@`;
    const files = fs.readdirSync(CACHE_DIR);
    const match = files.find((file) => {
      const full = path.join(CACHE_DIR, file);
      return (
        file.startsWith(prefix) && fs.existsSync(path.join(full, MAIN_FILE))
      );
    });
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

  private getCdnDependencyFilePath(
    specifier: string,
    name: string,
    version: string,
    packageDir = this.getPackageArtifactDir(name, version),
  ): string {
    const withoutQuery = specifier.split('?')[0] || specifier;
    const basename = path.basename(withoutQuery) || 'index';
    const safeBase = basename.replace(/[^a-zA-Z0-9._-]/g, '_');
    const hash = createHash('sha1')
      .update(specifier)
      .digest('hex')
      .slice(0, 12);
    return path.join(packageDir, DEPS_DIR, `cdn-${hash}-${safeBase}.mjs`);
  }

  private async fetchAndPrepareCdnModule(
    specifier: string,
    name: string,
    version: string,
    packageDir = this.getPackageArtifactDir(name, version),
    currentDir = packageDir,
    seen = new Set<string>(),
    dependencyHints: CdnDependencyHints = new Map(),
  ): Promise<string> {
    specifier = this.applyDependencyHintToCdnSpecifier(
      specifier,
      dependencyHints,
    );
    if (seen.has(specifier)) {
      const existingPath = this.getCdnDependencyFilePath(
        specifier,
        name,
        version,
        packageDir,
      );
      if (fs.existsSync(existingPath)) {
        return fs.readFileSync(existingPath, 'utf-8');
      }
      return '';
    }
    seen.add(specifier);

    let code = await this.loadModuleSource(specifier);
    await this.collectDependencyHintsForSpecifier(specifier, dependencyHints);
    code = this.suppressMissingModuleConsoleErrors(code);
    code = this.injectNodeEsmGlobals(code);
    code = await this.rewriteCdnSpecifiers(
      code,
      specifier,
      name,
      version,
      packageDir,
      currentDir,
      seen,
      dependencyHints,
    );
    return code;
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
    seen: Set<string>,
    dependencyHints: CdnDependencyHints,
  ): Promise<string> {
    const importPattern =
      /\b(from\s*["']|import\s*["'])(\/[^"']+|file:\/\/[^"']+|\.{1,2}\/[^"']+)(["'])/g;
    const specifiers = new Set<string>();
    for (const match of code.matchAll(importPattern)) {
      specifiers.add(match[2]);
    }

    for (const specifier of specifiers) {
      const resolvedSpecifier = this.resolveCdnImportSpecifier(
        specifier,
        sourceSpecifier,
      );
      await this.collectDependencyHintsForSpecifier(
        resolvedSpecifier,
        dependencyHints,
      );
    }

    const replacements = new Map<string, string>();
    for (const specifier of specifiers) {
      const resolvedSpecifier = this.applyDependencyHintToCdnSpecifier(
        this.resolveCdnImportSpecifier(specifier, sourceSpecifier),
        dependencyHints,
      );
      const depPath = this.getCdnDependencyFilePath(
        resolvedSpecifier,
        name,
        version,
        packageDir,
      );
      if (this.isNativeCdnImport(resolvedSpecifier)) {
        fs.writeFileSync(depPath, NATIVE_CDN_STUB_SOURCE, 'utf-8');
      } else if (!fs.existsSync(depPath)) {
        const depCode = await this.fetchAndPrepareCdnModule(
          resolvedSpecifier,
          name,
          version,
          packageDir,
          path.dirname(depPath),
          seen,
          dependencyHints,
        );
        fs.writeFileSync(depPath, depCode, 'utf-8');
      }
      replacements.set(specifier, this.toRelativeImport(depPath, currentDir));
    }

    return code.replace(importPattern, (full, prefix, specifier, suffix) => {
      const replacement = replacements.get(specifier);
      return replacement ? `${prefix}${replacement}${suffix}` : full;
    });
  }

  private resolveCdnImportSpecifier(
    specifier: string,
    sourceSpecifier: string,
  ): string {
    if (!specifier.startsWith('./') && !specifier.startsWith('../')) {
      return specifier;
    }
    if (sourceSpecifier.startsWith('file://')) {
      const sourceUrl = new URL(sourceSpecifier);
      const resolved = new URL(specifier, sourceUrl);
      return resolved.href;
    }
    const [sourcePath, sourceQuery] = sourceSpecifier.split('?');
    const resolvedPath = path.posix.normalize(
      path.posix.join(path.posix.dirname(sourcePath || '/'), specifier),
    );
    return sourceQuery ? `${resolvedPath}?${sourceQuery}` : resolvedPath;
  }

  private isNativeCdnImport(specifier: string): boolean {
    return specifier.split('?')[0].endsWith('.node');
  }

  private parseCdnPackageSpecifier(
    specifier: string,
  ): ParsedCdnPackageSpecifier | null {
    if (!specifier.startsWith('/')) return null;
    const [packagePath, query = ''] = specifier.split('?');
    const parts = packagePath.split('/').filter(Boolean);
    if (parts.length === 0) return null;

    let name: string;
    let version: string | null = null;
    if (parts[0].startsWith('@')) {
      if (!parts[1]) return null;
      const versionIndex = parts[1].lastIndexOf('@');
      name =
        versionIndex > 0
          ? `${parts[0]}/${parts[1].slice(0, versionIndex)}`
          : `${parts[0]}/${parts[1]}`;
      version = versionIndex > 0 ? parts[1].slice(versionIndex + 1) : null;
    } else {
      const versionIndex = parts[0].lastIndexOf('@');
      name = versionIndex > 0 ? parts[0].slice(0, versionIndex) : parts[0];
      version = versionIndex > 0 ? parts[0].slice(versionIndex + 1) : null;
    }

    return { name, version, packagePath, query };
  }

  private applyDependencyHintToCdnSpecifier(
    specifier: string,
    dependencyHints: CdnDependencyHints,
  ): string {
    const parsed = this.parseCdnPackageSpecifier(specifier);
    if (!parsed || parsed.version) return specifier;

    const hintedVersion = dependencyHints.get(parsed.name);
    if (!hintedVersion) return specifier;

    const prefix = parsed.name.startsWith('@')
      ? `/${parsed.name}@${hintedVersion}`
      : `/${parsed.name}@${hintedVersion}`;
    const nameParts = parsed.name.split('/');
    const pathParts = parsed.packagePath.split('/').filter(Boolean);
    const rest = pathParts.slice(nameParts.length).join('/');
    const hintedPath = rest ? `${prefix}/${rest}` : prefix;
    return parsed.query ? `${hintedPath}?${parsed.query}` : hintedPath;
  }

  private async collectDependencyHintsForSpecifier(
    specifier: string,
    dependencyHints: CdnDependencyHints,
  ): Promise<void> {
    const parsed = this.parseCdnPackageSpecifier(specifier);
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

  private toRelativeImport(depPath: string, currentDir: string): string {
    const relative = path.relative(currentDir, depPath);
    return relative.startsWith('.') ? relative : `./${relative}`;
  }

  private shouldFallbackToWorkerRuntime(error: any): boolean {
    const message = extractErrorMessage(error);
    return WORKER_RUNTIME_ERROR_PATTERNS.some((pattern) =>
      message.includes(pattern),
    );
  }

  private isEsmReExportCycleError(error: any): boolean {
    return extractErrorMessage(error).includes(
      'Detected cycle while resolving name',
    );
  }

  private async refetchAndImportPackage(
    name: string,
    version: string,
    filePath: string,
    firstError: any,
  ): Promise<any> {
    const target: CdnBundleTarget = this.isEsmReExportCycleError(firstError)
      ? 'es2022'
      : 'node';
    this.deletePackageArtifacts(name);
    await this.fetchAndWriteBundle(name, version, target);

    try {
      return await this.importFromFile(filePath, name);
    } catch (retryError) {
      if (target === 'node' && this.isEsmReExportCycleError(retryError)) {
        this.deletePackageArtifacts(name);
        await this.fetchAndWriteBundle(name, version, 'es2022');
        try {
          return await this.importFromFile(filePath, name);
        } catch (es2022Error) {
          if (this.shouldFallbackToWorkerRuntime(es2022Error)) {
            return { __enfyraRuntime: 'worker', name, version };
          }
          throw es2022Error;
        }
      }
      if (this.shouldFallbackToWorkerRuntime(retryError)) {
        return { __enfyraRuntime: 'worker', name, version };
      }
      throw retryError;
    }
  }

  private suppressMissingModuleConsoleErrors(code: string): string {
    return code.replace(
      /default:console\.error\('module "'\+n\+'" not found'\);return null;/g,
      'default:return null;',
    );
  }

  private injectNodeEsmGlobals(code: string): string {
    if (!code.includes('__dirname') && !code.includes('__filename')) {
      return code;
    }
    return [
      'import { fileURLToPath as __enfyraFileURLToPath } from "node:url";',
      'import { dirname as __enfyraDirname } from "node:path";',
      'const __filename = __enfyraFileURLToPath(import.meta.url);',
      'const __dirname = __enfyraDirname(__filename);',
      code,
    ].join('\n');
  }

  private async importFromFile(filePath: string, name: string): Promise<any> {
    try {
      const stat = fs.statSync(filePath);
      const fileUrl = `${pathToFileURL(filePath).href}?v=${stat.mtimeMs}-${stat.size}`;
      const mod = (await this.withTimeout(
        new Function('specifier', 'return import(specifier)')(fileUrl),
        CDN_IMPORT_TIMEOUT_MS,
        `CDN import timed out for ${name}`,
      )) as any;
      return mod.default !== undefined ? mod.default : mod;
    } catch (error) {
      this.logger.error(
        `Failed to import ${name} from ${filePath}: ${extractErrorMessage(error)}`,
      );
      throw error;
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
