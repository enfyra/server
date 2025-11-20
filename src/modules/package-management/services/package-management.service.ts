import { Injectable } from '@nestjs/common';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs/promises';
import * as path from 'path';

const execAsync = promisify(exec);

interface PackageInstallRequest {
  name: string;
  type: 'App' | 'Backend';
  version?: string;
  flags?: string;
}

interface PackageUpdateRequest {
  name: string;
  type: 'App' | 'Backend';
  currentVersion: string;
  newVersion: string;
}

interface PackageUninstallRequest {
  name: string;
  type: 'App' | 'Backend';
}

interface InstallationResult {
  version: string;
  description?: string;
}

@Injectable()
export class PackageManagementService {
  private getPackageManager(): string {
    // Check environment variable first
    const envPkgManager = process.env.PACKAGE_MANAGER;
    if (envPkgManager) {
      return envPkgManager;
    }

    // Auto-detect based on lock files
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

    // Default fallback
    return 'npm';
  }
  async installPackage(
    request: PackageInstallRequest,
  ): Promise<InstallationResult> {
    const { name, type, version = 'latest', flags = '' } = request;

    if (type === 'App') {
      // App packages are not handled by backend - skip
      return {
        version: version === 'latest' ? '1.0.0' : version,
        description: `App package ${name} (skipped - handled by frontend)`,
      };
    }

    if (type === 'Backend') {
      return await this.installBackendPackage(name, version, flags);
    }

    throw new Error(`Unsupported package type: ${type}`);
  }

  async updatePackage(
    request: PackageUpdateRequest,
  ): Promise<InstallationResult> {
    const { name, type, newVersion } = request;

    if (type === 'App') {
      // App packages are not handled by backend - skip
      return {
        version: newVersion,
        description: `App package ${name} (skipped - handled by frontend)`,
      };
    }

    // For backend updates, reuse the install logic with the new version
    return await this.installPackage({
      name,
      type,
      version: newVersion,
    });
  }

  async uninstallPackage(request: PackageUninstallRequest): Promise<void> {
    const { name, type } = request;

    if (type === 'App') {
      // App packages are not handled by backend - skip
      return;
    }

    if (type === 'Backend') {
      await this.uninstallBackendPackage(name);
      return;
    }

    throw new Error(`Unsupported package type: ${type}`);
  }

  private async installBackendPackage(
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

      // Get package info
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

      // If npm install fails with empty output, try alternative approach
      if (error.code === 1 && error.stdout === '' && error.stderr === '') {
        try {
          // Try with --no-package-lock flag
          const altCommand = `npm install ${packageSpec} --no-package-lock ${flags}`.trim();

          const { stderr: altStderr } = await execAsync(altCommand, {
            cwd: process.cwd(),
            timeout: 30000,
          });

          if (altStderr && !altStderr.includes('WARN')) {
            throw new Error(altStderr);
          }

          // Get package info
          const packageInfo = await this.getPackageInfo(name);
          return {
            version: packageInfo.version,
            description: packageInfo.description,
          };
        } catch (altError) {
          console.error(`Alternative install also failed:`, altError.message);

          // Final diagnostic check
          try {
            const { stdout: versionOutput } = await execAsync('npm --version', {
              cwd: process.cwd(),
              timeout: 10000,
            });

            const { stdout: configOutput } = await execAsync('npm config get registry', {
              cwd: process.cwd(),
              timeout: 10000,
            });
          } catch (diagError) {
            console.error(`Diagnostic check failed:`, diagError.message);
          }

          // Try with official npm registry
          try {
            const registryCommand = `npm install ${packageSpec} --registry https://registry.npmjs.org/ ${flags}`.trim();

            const { stderr: registryStderr } = await execAsync(registryCommand, {
              cwd: process.cwd(),
              timeout: 30000,
            });

            if (registryStderr && !registryStderr.includes('WARN')) {
              throw new Error(registryStderr);
            }

            // Get package info
            const packageInfo = await this.getPackageInfo(name);
            return {
              version: packageInfo.version,
              description: packageInfo.description,
            };
          } catch (registryError) {
            console.error(`Official registry install also failed:`, registryError.message);

            // Final check: try a simple npm command
            try {
              await execAsync('npm list --depth=0', {
                cwd: process.cwd(),
                timeout: 10000,
              });
            } catch (testError) {
              console.error(`Basic npm commands also failing:`, testError.message);
            }

            throw new Error(`All npm install attempts failed. This appears to be an npm environment issue. Try running 'npm install lodash' manually in terminal to diagnose.`);
          }
        }
      }

      throw error;
    }
  }


  private async uninstallBackendPackage(name: string): Promise<void> {
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
        timeout: 120000, // 2 minutes timeout
      });


      if (stderr && !stderr.includes('WARN') && !stderr.includes('warning') && !stderr.includes('not found')) {
        throw new Error(stderr);
      }
    } catch (error) {
      // Silently skip if package not found in node_modules
      const errorMsg = error.message || error.toString();

      // Check if this is a "package not found" scenario
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
      // If we can't read package.json, try npm info
      try {
        const { stdout } = await execAsync(
          `npm info ${packageName} version description --json`,
        );
        const info = JSON.parse(stdout);
        return {
          version: info.version,
          description: info.description,
        };
      } catch (npmError) {
        throw new Error(`Could not get package info: ${error.message}`);
      }
    }
  }

  async listInstalledPackages(): Promise<any[]> {
    try {
      const { stdout } = await execAsync('npm list --json --depth=0');
      const npmList = JSON.parse(stdout);

      const packages = [];
      for (const [name, info] of Object.entries(npmList.dependencies || {})) {
        packages.push({
          name,
          version: (info as any).version,
          description: (info as any).description || '',
        });
      }

      return packages;
    } catch (error) {
      throw new Error(`Failed to list installed packages: ${error.message}`);
    }
  }

  async searchPackages(query: string): Promise<any[]> {
    try {
      const { stdout } = await execAsync(`npm search ${query} --json`);
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
      // Check if package is listed in project's package.json dependencies
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
