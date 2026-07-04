import { readFileSync } from 'node:fs';
import { join } from 'node:path';

function normalizePackageVersion(value: unknown): string {
  return String(value || '').trim().replace(/^v/i, '');
}

function readPackageVersion(): string {
  const packageJson = JSON.parse(
    readFileSync(join(process.cwd(), 'package.json'), 'utf8'),
  ) as { version?: string };
  return normalizePackageVersion(packageJson.version);
}

const ENFYRA_VERSION = readPackageVersion();

export function getEnfyraVersion(): string {
  return ENFYRA_VERSION;
}
