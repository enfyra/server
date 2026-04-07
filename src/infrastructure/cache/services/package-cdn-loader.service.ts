import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { pathToFileURL } from 'url';

const CDN_BASE = 'https://esm.sh';
const CACHE_DIR = path.join(os.tmpdir(), 'enfyra-pkg-cache');

export function extractErrorMessage(error: any): string {
  const parts: string[] = [];
  let current = error;
  while (current) {
    if (current.message) parts.push(current.message);
    current = current.cause;
  }
  return parts.join(' → ') || 'Unknown error';
}

@Injectable()
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

    const filePath = this.getTempFilePath(name, version);

    if (!fs.existsSync(filePath)) {
      await this.fetchAndWriteBundle(name, version, filePath);
    }

    const mod = await this.importFromFile(filePath, name);
    this.moduleCache.set(cacheKey, mod);
    return mod;
  }

  async preloadPackages(packages: Array<{ name: string; version: string }>): Promise<void> {
    for (const pkg of packages) {
      try {
        await this.loadPackage(pkg.name, pkg.version);
        this.logger.log(`Preloaded: ${pkg.name}@${pkg.version}`);
      } catch (error) {
        this.logger.error(`Failed to preload ${pkg.name}@${pkg.version}: ${extractErrorMessage(error)}`);
      }
    }
  }

  async invalidatePackage(name: string, newVersion?: string): Promise<void> {
    for (const key of this.moduleCache.keys()) {
      if (key === name || key.startsWith(`${name}@`)) {
        this.moduleCache.delete(key);
      }
    }

    const prefix = `${name.replace(/[^a-zA-Z0-9]/g, '_')}@`;
    try {
      const files = fs.readdirSync(CACHE_DIR);
      for (const file of files) {
        if (file.startsWith(prefix)) {
          fs.unlinkSync(path.join(CACHE_DIR, file));
        }
      }
    } catch {}

    if (newVersion) {
      try {
        await this.loadPackage(name, newVersion);
      } catch (error) {
        this.logger.error(`Failed to reload ${name}@${newVersion}: ${extractErrorMessage(error)}`);
      }
    }
  }

  getPackageSources(names: string[]): Array<{ name: string; safeName: string; sourceCode: string }> {
    const results: Array<{ name: string; safeName: string; sourceCode: string }> = [];
    for (const name of names) {
      const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
      const prefix = `${safeName}@`;
      try {
        const files = fs.readdirSync(CACHE_DIR);
        const match = files.find((f) => f.startsWith(prefix) && f.endsWith('.mjs'));
        if (match) {
          const sourceCode = fs.readFileSync(path.join(CACHE_DIR, match), 'utf-8');
          results.push({ name, safeName, sourceCode });
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
        fs.unlinkSync(path.join(CACHE_DIR, file));
      }
    } catch {}
  }

  private getTempFilePath(name: string, version: string): string {
    const safeName = name.replace(/[^a-zA-Z0-9]/g, '_');
    return path.join(CACHE_DIR, `${safeName}@${version}.mjs`);
  }

  private async fetchAndWriteBundle(name: string, version: string, filePath: string): Promise<void> {
    const spec = `${name}@${version}`;
    const entryUrl = `${CDN_BASE}/${spec}?bundle&target=node`;

    this.logger.log(`Fetching from CDN: ${spec}`);

    let entryRes: Response;
    try {
      entryRes = await fetch(entryUrl);
    } catch (error) {
      throw new Error(`CDN fetch failed for ${spec}: ${extractErrorMessage(error)}`);
    }

    if (!entryRes.ok) {
      throw new Error(`CDN fetch failed for ${spec}: ${entryRes.status} ${entryRes.statusText}`);
    }

    let code = await entryRes.text();

    if (code.length < 1024) {
      const bundlePath = code.match(/export\s+(?:\*|\{[^}]*\})\s+from\s*["'](\/[^"']+)["']/);
      if (bundlePath?.[1]) {
        try {
          const bundleRes = await fetch(`${CDN_BASE}${bundlePath[1]}`);
          if (bundleRes.ok) {
            code = await bundleRes.text();
          }
        } catch (error) {
          this.logger.warn(`Bundle follow-up fetch failed for ${spec}: ${extractErrorMessage(error)}`);
        }
      }
    }

    fs.writeFileSync(filePath, code, 'utf-8');
  }

  private async importFromFile(filePath: string, name: string): Promise<any> {
    try {
      const fileUrl = pathToFileURL(filePath).href;
      const mod = await (new Function('specifier', 'return import(specifier)'))(fileUrl);
      return mod.default !== undefined ? mod.default : mod;
    } catch (error) {
      this.logger.error(`Failed to import ${name} from ${filePath}: ${extractErrorMessage(error)}`);
      throw error;
    }
  }
}
