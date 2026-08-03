import { describe, expect, it } from 'vitest';
import {
  assertBootstrapMatrixDatabaseName,
  createBootstrapMatrixDatabaseName,
  deriveBootstrapMatrixCaseSeed,
  resolveBootstrapMatrixConfig,
} from './bootstrap-randomized-matrix.config';

describe('randomized bootstrap matrix config', () => {
  it('uses deterministic replay config and de-duplicates databases', () => {
    expect(
      resolveBootstrapMatrixConfig(
        {
          BOOTSTRAP_MATRIX_SEED: '1234',
          BOOTSTRAP_MATRIX_CASES: '4',
          MATRIX_DATABASES: 'mongodb,postgres,mongodb',
        },
        99,
      ),
    ).toEqual({
      seed: 1234,
      cases: 4,
      databases: ['mongodb', 'postgres'],
    });
  });

  it('derives stable distinct case seeds', () => {
    expect(deriveBootstrapMatrixCaseSeed(1234, 0)).toBe(1234);
    expect(deriveBootstrapMatrixCaseSeed(1234, 1)).toBe(2_654_436_995);
    expect(deriveBootstrapMatrixCaseSeed(1234, 1)).not.toBe(
      deriveBootstrapMatrixCaseSeed(1234, 2),
    );
  });

  it.each([
    { BOOTSTRAP_MATRIX_SEED: '1.2' },
    { BOOTSTRAP_MATRIX_SEED: '9007199254740992' },
    { BOOTSTRAP_MATRIX_CASES: '0' },
    { BOOTSTRAP_MATRIX_CASES: '-1' },
    { MATRIX_DATABASES: 'postgres,sqlite' },
    { MATRIX_DATABASES: ',,' },
  ])('rejects invalid config %#', (environment) => {
    expect(() => resolveBootstrapMatrixConfig(environment, 42)).toThrow();
  });

  it('creates a bounded disposable database name', () => {
    const name = createBootstrapMatrixDatabaseName(
      'postgres',
      1234,
      'A1B2-C3D4-E5F6',
    );
    expect(name).toBe('enfyra_bootstrap_matrix_postgres_ya_a1b2c3d4e5');
    expect(name.length).toBeLessThanOrEqual(63);
    expect(() => assertBootstrapMatrixDatabaseName(name)).not.toThrow();
  });

  it.each([
    'enfyra',
    'enfyra_bootstrap_matrix_postgres',
    'enfyra_bootstrap_matrix_postgres_seed_../../prod',
    'enfyra_bootstrap_matrix_sqlite_seed_id',
  ])('refuses unsafe cleanup target %s', (name) => {
    expect(() => assertBootstrapMatrixDatabaseName(name)).toThrow(
      'Refusing unsafe bootstrap matrix database target',
    );
  });
});
