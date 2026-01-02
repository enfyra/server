import { Injectable } from '@nestjs/common';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';

const execAsync = promisify(exec);

interface PackageInstallRequest {
  name: string;
  type: 'App' | 'Server';
  version?: string;
  flags?: string;
}

interface PackageUpdateRequest {
  name: string;
  type: 'App' | 'Server';
  currentVersion: string;
  newVersion: string;
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
  private getPackageManager(): string {
    const envPkgManager = process.env.PACKAGE_MANAGER;
    if (envPkgManager) {
      return envPkgManager;
    }

    const fs = require('fs');
    const path = require('path');

    if (fs.existsSync(path.join(process.cwd(), 'bun.lockb'))) {
      return 'bun';
    }
    if (fs.existsSync(path.join(process.cwd(), 'yarn.lock'))) {
      return 'yarn';
    }
    if (fs.existsSync(path.join(process.cwd(), 'package-lock.json'))) {
      return 'npm';
    }
    if (fs.existsSync(path.join(process.cwd(), 'pnpm-lock.yaml'))) {
      return 'pnpm';
    }

    return 'npm';
  }
  async installPackage(
    request: PackageInstallRequest,
  ): Promise<InstallationResult> {
    const { name, type, version = 'latest', flags = '' } = request;

    if (type === 'App') {
      return {
        version: version === 'latest' ? '1.0.0' : version,
        description: `App package ${name} (skipped - handled by frontend)`,
      };
    }

    if (type === 'Server') {
      return await this.installServerPackage(name, version, flags);
    }

    throw new Error(`Unsupported package type: ${type}`);
  }

  async updatePackage(
    request: PackageUpdateRequest,
  ): Promise<InstallationResult> {
    const { name, type, newVersion } = request;

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
  ): Promise<InstallationResult> {
    const packageManager = this.getPackageManager();
    const packageSpec = version === 'latest' ? name : `${name}@${version}`;

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
        timeout: 30000,
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
          timeout: 30000,
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


  async isPackageInstalled(packageName: string): Promise<boolean> {
    try {
      const projectPackageJsonPath = path.join(process.cwd(), 'package.json');
      const packageJsonContent = await fs.readFile(projectPackageJsonPath, 'utf-8');
      const packageJson = JSON.parse(packageJsonContent);

      const allDependencies = {
        ...(packageJson.dependencies || {}),
        ...(packageJson.devDependencies || {}),
        ...(packageJson.peerDependencies || {}),
        ...(packageJson.optionalDependencies || {}),
      };

      return packageName in allDependencies;
    } catch (error) {
      console.error(`Error checking package installation status for ${packageName}:`, error);
      return false;
    }
  }
}
