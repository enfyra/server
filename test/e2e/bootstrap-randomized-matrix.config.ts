import { randomBytes, randomUUID } from 'node:crypto';
import type {
  BootstrapMatrixDatabase,
  BootstrapRandomizedMatrixConfig,
} from './types/bootstrap-randomized-matrix.types';

export const BOOTSTRAP_MATRIX_DATABASE_PREFIX = 'enfyra_bootstrap_matrix_';

const SUPPORTED_DATABASES = new Set<BootstrapMatrixDatabase>([
  'postgres',
  'mysql',
  'mongodb',
]);

function parseSafeInteger(value: string, label: string): number {
  if (!/^-?\d+$/.test(value)) {
    throw new Error(`${label} must be a safe integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${label} must be a safe integer`);
  }
  return parsed;
}

export function createBootstrapMatrixSeed(): number {
  return randomBytes(6).readUIntBE(0, 6);
}

export function resolveBootstrapMatrixConfig(
  environment: NodeJS.ProcessEnv = process.env,
  fallbackSeed = createBootstrapMatrixSeed(),
): BootstrapRandomizedMatrixConfig {
  const seed = environment.BOOTSTRAP_MATRIX_SEED
    ? parseSafeInteger(
        environment.BOOTSTRAP_MATRIX_SEED,
        'BOOTSTRAP_MATRIX_SEED',
      )
    : fallbackSeed;
  const cases = environment.BOOTSTRAP_MATRIX_CASES
    ? parseSafeInteger(
        environment.BOOTSTRAP_MATRIX_CASES,
        'BOOTSTRAP_MATRIX_CASES',
      )
    : 3;
  if (cases <= 0) {
    throw new Error('BOOTSTRAP_MATRIX_CASES must be a positive integer');
  }

  const requested = (environment.MATRIX_DATABASES || 'postgres,mysql,mongodb')
    .split(',')
    .map((database) => database.trim())
    .filter(Boolean);
  if (requested.length === 0) {
    throw new Error('MATRIX_DATABASES must include at least one database');
  }
  const unsupported = requested.filter(
    (database) => !SUPPORTED_DATABASES.has(database as BootstrapMatrixDatabase),
  );
  if (unsupported.length > 0) {
    throw new Error(
      `Unsupported MATRIX_DATABASES value: ${unsupported.join(', ')}`,
    );
  }

  return {
    seed,
    cases,
    databases: [...new Set(requested as BootstrapMatrixDatabase[])],
  };
}

export function deriveBootstrapMatrixCaseSeed(
  baseSeed: number,
  caseIndex: number,
): number {
  if (
    !Number.isSafeInteger(baseSeed) ||
    !Number.isInteger(caseIndex) ||
    caseIndex < 0
  ) {
    throw new Error(
      'Bootstrap matrix seed derivation requires a safe seed and non-negative case index',
    );
  }
  const modulus = BigInt(Number.MAX_SAFE_INTEGER);
  const normalized = ((BigInt(baseSeed) % modulus) + modulus) % modulus;
  return Number((normalized + BigInt(caseIndex) * 2_654_435_761n) % modulus);
}

export function createBootstrapMatrixDatabaseName(
  database: BootstrapMatrixDatabase,
  seed: number,
  id = randomUUID(),
): string {
  if (!Number.isSafeInteger(seed)) {
    throw new Error('Bootstrap matrix database seed must be a safe integer');
  }
  const seedPart = (seed >>> 0).toString(36);
  const idPart = id
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '')
    .slice(0, 10);
  const name = `${BOOTSTRAP_MATRIX_DATABASE_PREFIX}${database}_${seedPart}_${idPart}`;
  assertBootstrapMatrixDatabaseName(name);
  return name;
}

export function assertBootstrapMatrixDatabaseName(name: string): void {
  if (
    !/^enfyra_bootstrap_matrix_(postgres|mysql|mongodb)_[a-z0-9]+_[a-z0-9]{1,10}$/.test(
      name,
    )
  ) {
    throw new Error(
      `Refusing unsafe bootstrap matrix database target: ${name}`,
    );
  }
}
