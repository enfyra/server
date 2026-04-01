import { Injectable, Logger } from '@nestjs/common';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';
import * as os from 'os';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { pkgLog } from './package-operation-logger';

const execAsync = promisify(exec);

const MACHINE_LOCK_KEY_PREFIX = 'pkg-install';
const MACHINE_LOCK_TTL = 5 * 60 * 1000;
const LOCK_WAIT_TIMEOUT = 6 * 60 * 1000;
const LOCK_POLL_INTERVAL = 1000;

interface PackageInstallRequest {
  name: string;
  type: 'App' | 'Server';
  version?: string;
  flags?: string;
  timeoutMs?: number;
}

interface PackageUpdateRequest {
  name: string;
  type: 'App' | 'Server';
  currentVersion: string;
  newVersion: string;
  timeoutMs?: number;
}

interface PackageUninstallRequest {
  name: string;
  type: 'App' | 'Server';
}

interface InstallationResult {
  version: string;
  description?: string;
}

@Injectable()
export class PackageManagementService {
  private readonly logger = new Logger(PackageManagementService.name);
  private readonly machineId: string;

  constructor(
    private readonly cacheService: CacheService,
  ) {
    this.machineId = os.hostname();
  }

  private getMachineLockKey(): string {
    return `${MACHINE_LOCK_KEY_PREFIX}:${this.machineId}`;
  }

  async acquireMachineLock(): Promise<boolean> {
    pkgLog('PkgMgmt', `acquireMachineLock: attempting (machineId=${this.machineId})`);
    const deadline = Date.now() + LOCK_WAIT_TIMEOUT;
    while (Date.now() < deadline) {
      const acquired = await this.cacheService.acquire(
        this.getMachineLockKey(),
        this.machineId,
        MACHINE_LOCK_TTL,
      );
      if (acquired) {
        pkgLog('PkgMgmt', `acquireMachineLock: ACQUIRED`);
        return true;
      }
      await new Promise((r) => setTimeout(r, LOCK_POLL_INTERVAL));
    }
    pkgLog('PkgMgmt', `acquireMachineLock: TIMEOUT`);
    this.logger.warn('Failed to acquire machine lock after timeout');
    return false;
  }

  async releaseMachineLock(): Promise<void> {
    pkgLog('PkgMgmt', `releaseMachineLock`);
    await this.cacheService.release(this.getMachineLockKey(), this.machineId);
  }

  async renewMachineLock(): Promise<void> {
    await this.cacheService.set(this.getMachineLockKey(), this.machineId, MACHINE_LOCK_TTL);
  }

  private getPackageManager(): string {
    const envPkgManager = process.env.PACKAGE_MANAGER;
    if (envPkgManager) {
      return envPkgManager;
    }

    const fsSync = require('fs');
    const pathSync = require('path');

    if (fsSync.existsSync(pathSync.join(process.cwd(), 'bun.lockb'))) {
      return 'bun';
    }
    if (fsSync.existsSync(pathSync.join(process.cwd(), 'yarn.lock'))) {
      return 'yarn';
    }
    if (fsSync.existsSync(pathSync.join(process.cwd(), 'package-lock.json'))) {
      return 'npm';
    }
    if (fsSync.existsSync(pathSync.join(process.cwd(), 'pnpm-lock.yaml'))) {
      return 'pnpm';
    }

    return 'npm';
  }

  async installBatch(packages: Array<{ name: string; version: string; timeoutMs?: number }>): Promise<void> {
    if (packages.length === 0) return;

    const packageManager = this.getPackageManager();
    const specs = packages.map((p) =>
      p.version === 'latest' ? p.name : `${p.name}@${p.version}`,
    );
    const specStr = specs.join(' ');

    let command: string;
    if (packageManager === 'bun') {
      command = `bun add ${specStr}`;
    } else if (packageManager === 'yarn') {
      command = `yarn add ${specStr}`;
    } else if (packageManager === 'pnpm') {
      command = `pnpm add ${specStr}`;
    } else {
      command = `npm install ${specStr} --legacy-peer-deps`;
    }

    const timeout = Math.max(60000, packages.length * 30000);

    try {
      pkgLog('PkgMgmt', `installBatch: ${command} (timeout=${timeout}ms)`);
      this.logger.log(`Batch installing ${packages.length} packages: ${specStr}`);
      const { stderr } = await execAsync(command, {
        cwd: process.cwd(),
        timeout,
      });

      if (stderr && !stderr.includes('WARN') && !stderr.includes('warning')) {
        pkgLog('PkgMgmt', `installBatch stderr`, stderr.substring(0, 500));
        this.logger.warn(`Batch install stderr: ${stderr.substring(0, 500)}`);
      }

      pkgLog('PkgMgmt', `installBatch OK: ${packages.length} packages`);
      this.logger.log(`Batch install completed for ${packages.length} packages`);
    } catch (error) {
      pkgLog('PkgMgmt', `installBatch FAILED`, { error: error.message, stderr: error.stderr?.substring(0, 500) });
      this.logger.error(`Batch install failed: ${error.message}`);
      this.logger.log('Falling back to individual installs...');

      for (const pkg of packages) {
        try {
          pkgLog('PkgMgmt', `Individual install: ${pkg.name}@${pkg.version}`);
          await this.installServerPackage(pkg.name, pkg.version, '', pkg.timeoutMs);
          pkgLog('PkgMgmt', `Individual install OK: ${pkg.name}`);
        } catch (e) {
          pkgLog('PkgMgmt', `Individual install FAILED: ${pkg.name}`, e.message);
          this.logger.error(`Individual install failed for ${pkg.name}: ${e.message}`);
        }
      }
    }
  }

  async uninstallOrphan(packageNames: string[]): Promise<void> {
    if (packageNames.length === 0) return;

    const packageManager = this.getPackageManager();
    const nameStr = packageNames.join(' ');

    let command: string;
    if (packageManager === 'bun') {
      command = `bun remove ${nameStr}`;
    } else if (packageManager === 'yarn') {
      command = `yarn remove ${nameStr}`;
    } else if (packageManager === 'pnpm') {
      command = `pnpm remove ${nameStr}`;
    } else {
      command = `npm uninstall ${nameStr} --legacy-peer-deps`;
    }

    try {
      this.logger.log(`Cleaning up orphan packages: ${nameStr}`);
      await execAsync(command, { cwd: process.cwd(), timeout: 120000 });
      this.logger.log(`Orphan cleanup completed`);
    } catch (error) {
      this.logger.warn(`Orphan cleanup failed (non-critical): ${error.message}`);
    }
  }

  async installPackage(
    request: PackageInstallRequest,
  ): Promise<InstallationResult> {
    const { name, type, version = 'latest', flags = '', timeoutMs } = request;
    pkgLog('PkgMgmt', `installPackage`, { name, type, version, flags, timeoutMs });

    if (type === 'App') {
      return {
        version: version === 'latest' ? '1.0.0' : version,
        description: `App package ${name} (skipped - handled by frontend)`,
      };
    }

    if (type === 'Server') {
      return await this.installServerPackage(name, version, flags, timeoutMs);
    }

    throw new Error(`Unsupported package type: ${type}`);
  }

  async updatePackage(
    request: PackageUpdateRequest,
  ): Promise<InstallationResult> {
    const { name, type, newVersion, timeoutMs } = request;

    if (type === 'App') {
      return {
        version: newVersion,
        description: `App package ${name} (skipped - handled by frontend)`,
      };
    }

    return await this.installPackage({
      name,
      type,
      version: newVersion,
      timeoutMs,
    });
  }

  async uninstallPackage(request: PackageUninstallRequest): Promise<void> {
    const { name, type } = request;

    if (type === 'App') {
      return;
    }

    if (type === 'Server') {
      await this.uninstallServerPackage(name);
      return;
    }

    throw new Error(`Unsupported package type: ${type}`);
  }

  private async installServerPackage(
    name: string,
    version: string,
    flags: string,
    timeoutMs?: number,
  ): Promise<InstallationResult> {
    const packageManager = this.getPackageManager();
    const packageSpec = version === 'latest' ? name : `${name}@${version}`;
    const installTimeout = timeoutMs || 60000;
    pkgLog('PkgMgmt', `installServerPackage: ${packageSpec} (pm=${packageManager}, timeout=${installTimeout}ms)`);

    let command: string;
    if (packageManager === 'bun') {
      command = `bun add ${packageSpec} ${flags}`.trim();
    } else if (packageManager === 'yarn') {
      command = `yarn add ${packageSpec} ${flags}`.trim();
    } else if (packageManager === 'pnpm') {
      command = `pnpm add ${packageSpec} ${flags}`.trim();
    } else {
      command = `npm install ${packageSpec} --legacy-peer-deps ${flags}`.trim();
    }

    try {
      const { stderr } = await execAsync(command, {
        cwd: process.cwd(),
        timeout: installTimeout,
      });

      if (stderr && !stderr.includes('WARN') && !stderr.includes('warning')) {
        throw new Error(stderr);
      }

      const packageInfo = await this.getPackageInfo(name);
      return {
        version: packageInfo.version,
        description: packageInfo.description,
      };
    } catch (error) {
      console.error(`Install failed for ${name}:`, error.message);
      console.error(`Install error details:`, {
        code: error.code,
        stdout: error.stdout,
        stderr: error.stderr,
        cmd: error.cmd
      });

      const errorMsg = error.message + ' ' + (error.stderr || '');
      const isCacheCorruption = errorMsg.includes('corrupt') ||
        errorMsg.includes('Extracting tar content') ||
        errorMsg.includes('ENOENT') ||
        errorMsg.includes('EINTEGRITY') ||
        errorMsg.includes('invalid package') ||
        errorMsg.includes('unexpected end of file') ||
        errorMsg.includes('zlib');

      if (isCacheCorruption) {
        console.log(`Detected ${packageManager} cache corruption, clearing cache...`);
        try {
          if (packageManager === 'yarn') {
            await execAsync('yarn cache clean', { cwd: process.cwd(), timeout: 60000 });
            const cachePaths = [
              path.join(process.cwd(), '.yarn', 'cache'),
              path.join(require('os').homedir(), '.cache', 'yarn'),
              '/home/node/.cache/yarn',
            ];
            for (const cachePath of cachePaths) {
              try {
                await fs.rm(cachePath, { recursive: true, force: true });
                console.log(`Cleared yarn cache at: ${cachePath}`);
              } catch (e) { /* ignore */ }
            }
          } else if (packageManager === 'npm') {
            await execAsync('npm cache clean --force', { cwd: process.cwd(), timeout: 60000 });
            const npmCachePath = require('os').homedir() + '/.npm/_cacache';
            try {
              await fs.rm(npmCachePath, { recursive: true, force: true });
              console.log(`Cleared npm cache at: ${npmCachePath}`);
            } catch (e) { /* ignore */ }
          } else if (packageManager === 'pnpm') {
            await execAsync('pnpm store prune', { cwd: process.cwd(), timeout: 60000 });
          } else if (packageManager === 'bun') {
            const bunCachePath = path.join(require('os').homedir(), '.bun', 'install', 'cache');
            try {
              await fs.rm(bunCachePath, { recursive: true, force: true });
              console.log(`Cleared bun cache at: ${bunCachePath}`);
            } catch (e) { /* ignore */ }
          }
        } catch (cacheError) {
          console.warn(`Failed to clear ${packageManager} cache:`, cacheError.message);
        }
      }

      try {
        let registryCommand: string;
        if (packageManager === 'bun') {
          registryCommand = `bun add ${packageSpec} --registry https://registry.npmjs.org/ ${flags}`.trim();
        } else if (packageManager === 'yarn') {
          registryCommand = `yarn add ${packageSpec} --registry https://registry.npmjs.org/ ${flags}`.trim();
        } else if (packageManager === 'pnpm') {
          registryCommand = `pnpm add ${packageSpec} --registry https://registry.npmjs.org/ ${flags}`.trim();
        } else {
          registryCommand = `npm install ${packageSpec} --registry https://registry.npmjs.org/ --legacy-peer-deps ${flags}`.trim();
        }

        const { stderr: registryStderr } = await execAsync(registryCommand, {
          cwd: process.cwd(),
          timeout: installTimeout,
        });

        if (registryStderr && !registryStderr.includes('WARN') && !registryStderr.includes('warning')) {
          throw new Error(registryStderr);
        }

        const packageInfo = await this.getPackageInfo(name);
        return {
          version: packageInfo.version,
          description: packageInfo.description,
        };
      } catch (registryError) {
        console.error(`Official registry install also failed:`, registryError.message);
        throw new Error(`All ${packageManager} install attempts failed for ${name}. Try running '${packageManager} add ${name}' manually in terminal to diagnose.`);
      }
    }
  }

  private async uninstallServerPackage(name: string): Promise<void> {
    pkgLog('PkgMgmt', `uninstallServerPackage: ${name}`);
    const packageManager = this.getPackageManager();

    let command: string;
    if (packageManager === 'bun') {
      command = `bun remove ${name}`;
    } else if (packageManager === 'yarn') {
      command = `yarn remove ${name}`;
    } else if (packageManager === 'pnpm') {
      command = `pnpm remove ${name}`;
    } else {
      command = `npm uninstall ${name} --legacy-peer-deps`;
    }

    try {
      const { stderr } = await execAsync(command, {
        cwd: process.cwd(),
        timeout: 120000,
      });

      if (stderr && !stderr.includes('WARN') && !stderr.includes('warning') && !stderr.includes('not found')) {
        throw new Error(stderr);
      }
    } catch (error) {
      const errorMsg = error.message || error.toString();

      const isPackageNotFound =
        errorMsg.includes('not found') ||
        errorMsg.includes('ENOENT') ||
        errorMsg.includes('no such file') ||
        errorMsg.includes('not installed') ||
        errorMsg.includes('npm ERR! Cannot read property') ||
        (error.code === 1 && error.stdout === '' && error.stderr === '');

      if (isPackageNotFound) {
        console.log(
          `Package ${name} not found in node_modules, skipping uninstall`,
        );
        return;
      }
      throw new Error(`npm uninstall failed: ${errorMsg}`);
    }
  }

  async getPackageInfo(
    packageName: string,
  ): Promise<{ version: string; description?: string }> {
    try {
      const packageJsonPath = path.join(
        process.cwd(),
        'node_modules',
        packageName,
        'package.json',
      );
      const packageJsonContent = await fs.readFile(packageJsonPath, 'utf-8');
      const packageJson = JSON.parse(packageJsonContent);

      return {
        version: packageJson.version,
        description: packageJson.description,
      };
    } catch (error) {
      try {
        const packageManager = this.getPackageManager();
        let command: string;

        if (packageManager === 'bun') {
          command = `bun pm info ${packageName} --json`;
        } else if (packageManager === 'yarn') {
          command = `yarn npm info ${packageName} --json`;
        } else if (packageManager === 'pnpm') {
          command = `pnpm info ${packageName} --json`;
        } else {
          command = `npm info ${packageName} version description --json`;
        }

        const { stdout } = await execAsync(command);
        const info = JSON.parse(stdout);
        return {
          version: info.version || info[packageName]?.version,
          description: info.description || info[packageName]?.description,
        };
      } catch (npmError) {
        throw new Error(`Could not get package info: ${error.message}`);
      }
    }
  }

  async listInstalledPackages(): Promise<any[]> {
    const packageManager = this.getPackageManager();
    let command: string;

    if (packageManager === 'bun') {
      command = 'bun pm ls --json';
    } else if (packageManager === 'yarn') {
      command = 'yarn list --json --depth=0';
    } else if (packageManager === 'pnpm') {
      command = 'pnpm list --json --depth=0';
    } else {
      command = 'npm list --json --depth=0';
    }

    try {
      const { stdout } = await execAsync(command);
      const list = JSON.parse(stdout);

      let dependencies: Record<string, any> = {};
      if (packageManager === 'npm' || packageManager === 'pnpm') {
        dependencies = list.dependencies || {};
      } else if (packageManager === 'yarn') {
        dependencies = list.dependencies?.trees?.reduce((acc: any, tree: any) => {
          if (tree.name) {
            const [name, version] = tree.name.split('@');
            acc[name] = { name, version: tree.version || version };
          }
          return acc;
        }, {}) || {};
      } else if (packageManager === 'bun') {
        dependencies = list.packages || {};
      }

      const packages = [];
      for (const [name, info] of Object.entries(dependencies)) {
        packages.push({
          name,
          version: (info as any).version || 'unknown',
          description: (info as any).description || '',
        });
      }

      return packages;
    } catch (error) {
      throw new Error(`Failed to list installed packages: ${error.message}`);
    }
  }

  async searchPackages(query: string): Promise<any[]> {
    const packageManager = this.getPackageManager();
    let command: string;

    if (packageManager === 'bun') {
      command = `bun pm search ${query} --json`;
    } else if (packageManager === 'yarn') {
      command = `yarn npm search ${query} --json`;
    } else if (packageManager === 'pnpm') {
      command = `pnpm search ${query} --json`;
    } else {
      command = `npm search ${query} --json`;
    }

    try {
      const { stdout } = await execAsync(command);
      const searchResults = JSON.parse(stdout);

      return searchResults.map((pkg: any) => ({
        name: pkg.name,
        version: pkg.version,
        description: pkg.description,
        keywords: pkg.keywords,
        author: pkg.author,
      }));
    } catch (error) {
      throw new Error(`Package search failed: ${error.message}`);
    }
  }

  isPackageInstalled(packageName: string): boolean {
    try {
      require.resolve(packageName);
      return true;
    } catch {
      return false;
    }
  }
}
