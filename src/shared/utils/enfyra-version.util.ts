import { readFileSync } from 'node:fs';
import { join } from 'node:path';

export function normalizeEnfyraVersion(value: unknown): string {
  return String(value || '')
    .trim()
    .replace(/^v/i, '');
}

function readPackageVersion(): string {
  const packageJson = JSON.parse(
    readFileSync(join(process.cwd(), 'package.json'), 'utf8'),
  ) as { version?: string };
  return normalizeEnfyraVersion(packageJson.version);
}

const ENFYRA_VERSION = readPackageVersion();

export function getEnfyraVersion(): string {
  return ENFYRA_VERSION;
}

export function isEnfyraVersion(value: unknown): boolean {
  const normalized = normalizeEnfyraVersion(value);
  return /^\d+(\.\d+)*(-[0-9A-Za-z.]+)*$/.test(normalized);
}

function versionSegments(value: unknown): { core: number[]; suffix: string[] } {
  const normalized = normalizeEnfyraVersion(value);
  const [core = '', ...suffix] = normalized.split('-');
  return {
    core: core.split('.').map((part) => {
      const parsed = Number.parseInt(part, 10);
      // A non-numeric segment must not compare as 0: that would make a corrupt
      // recorded version look like a real, very old release.
      return Number.isNaN(parsed) ? -1 : parsed;
    }),
    suffix,
  };
}

/**
 * Compares two Enfyra versions held as strings. Numeric segments compare as
 * numbers so `2.2.9` precedes `2.2.10`, and a suffixed release follows its base
 * release so `2.2.19-patch-1` is newer than `2.2.19`.
 */
export function compareEnfyraVersions(left: unknown, right: unknown): number {
  const a = versionSegments(left);
  const b = versionSegments(right);

  const coreLength = Math.max(a.core.length, b.core.length);
  for (let index = 0; index < coreLength; index++) {
    const difference = (a.core[index] ?? 0) - (b.core[index] ?? 0);
    if (difference !== 0) return difference < 0 ? -1 : 1;
  }

  if (a.suffix.length === 0 && b.suffix.length === 0) return 0;
  if (a.suffix.length === 0) return -1;
  if (b.suffix.length === 0) return 1;

  const suffixLength = Math.max(a.suffix.length, b.suffix.length);
  for (let index = 0; index < suffixLength; index++) {
    const leftPart = a.suffix[index];
    const rightPart = b.suffix[index];
    if (leftPart === undefined) return -1;
    if (rightPart === undefined) return 1;

    const leftNumber = Number.parseInt(leftPart, 10);
    const rightNumber = Number.parseInt(rightPart, 10);
    if (!Number.isNaN(leftNumber) && !Number.isNaN(rightNumber)) {
      if (leftNumber !== rightNumber) return leftNumber < rightNumber ? -1 : 1;
      continue;
    }

    const difference = leftPart.localeCompare(rightPart);
    if (difference !== 0) return difference < 0 ? -1 : 1;
  }

  return 0;
}

export function isEnfyraVersionNewer(
  candidate: unknown,
  reference: unknown,
): boolean {
  return compareEnfyraVersions(candidate, reference) > 0;
}
