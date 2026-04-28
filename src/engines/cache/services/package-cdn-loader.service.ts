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
const NATIVE_CDN_STUB_SOURCE =
  'export function getCPUInfo() { return {}; }\nexport default { getCPUInfo };\n';

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
      await this.fetchAndWriteBundle(name, version);
    }

    let mod: any;
    try {
      mod = await this.importFromFile(filePath, name);
    } catch (error) {
      try {
        fs.unlinkSync(filePath);
      } catch {}
      this.deletePackageArtifacts(name);
      await this.fetchAndWriteBundle(name, version);
      try {
        mod = await this.importFromFile(filePath, name);
      } catch (retryError) {
        if (this.shouldFallbackToWorkerRuntime(retryError)) {
          mod = { __enfyraRuntime: 'worker', name, version };
        } else {
          throw retryError;
        }
      }
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
  ): Promise<void> {
    const spec = `${name}@${version}`;
    const entryPath = `/${spec}?bundle&target=node`;
    const packageDir = this.getPackageArtifactDir(name, version);
    const tempDir = `${packageDir}.tmp-${process.pid}-${Date.now()}`;

    this.logger.log(`Fetching from CDN: ${spec}`);

    fs.rmSync(tempDir, { recursive: true, force: true });
    fs.mkdirSync(path.join(tempDir, DEPS_DIR), { recursive: true });

    const code = await this.fetchAndPrepareCdnModule(
      entryPath,
      name,
      version,
      tempDir,
      tempDir,
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
  ): Promise<string> {
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
    }
    seen.add(specifier);

    let code = await this.loadModuleSource(specifier);
    code = this.suppressMissingModuleConsoleErrors(code);
    code = this.injectNodeEsmGlobals(code);
    code = await this.rewriteCdnSpecifiers(
      code,
      name,
      version,
      packageDir,
      currentDir,
      seen,
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
    try {
      res = await fetch(url);
    } catch (error) {
      throw new Error(
        `CDN fetch failed for ${cdnPath}: ${extractErrorMessage(error)}`,
      );
    }

    if (!res.ok) {
      throw new Error(
        `CDN fetch failed for ${cdnPath}: ${res.status} ${res.statusText}`,
      );
    }

    return res.text();
  }

  private async rewriteCdnSpecifiers(
    code: string,
    name: string,
    version: string,
    packageDir: string,
    currentDir: string,
    seen: Set<string>,
  ): Promise<string> {
    const importPattern =
      /\b(from\s*["']|import\s*["'])(\/[^"']+|file:\/\/[^"']+)(["'])/g;
    const specifiers = new Set<string>();
    for (const match of code.matchAll(importPattern)) {
      specifiers.add(match[2]);
    }

    const replacements = new Map<string, string>();
    for (const specifier of specifiers) {
      const depPath = this.getCdnDependencyFilePath(
        specifier,
        name,
        version,
        packageDir,
      );
      if (this.isNativeCdnImport(specifier)) {
        fs.writeFileSync(depPath, NATIVE_CDN_STUB_SOURCE, 'utf-8');
      } else if (!fs.existsSync(depPath)) {
        const depCode = await this.fetchAndPrepareCdnModule(
          specifier,
          name,
          version,
          packageDir,
          path.dirname(depPath),
          seen,
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

  private isNativeCdnImport(specifier: string): boolean {
    return specifier.split('?')[0].endsWith('.node');
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
      const fileUrl = pathToFileURL(filePath).href;
      const mod = await new Function('specifier', 'return import(specifier)')(
        fileUrl,
      );
      return mod.default !== undefined ? mod.default : mod;
    } catch (error) {
      this.logger.error(
        `Failed to import ${name} from ${filePath}: ${extractErrorMessage(error)}`,
      );
      throw error;
    }
  }
}
