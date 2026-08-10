import { describe, expect, it } from 'vitest';
import { classifyDynamicDatabaseError } from '../../src/modules/dynamic-api/utils/database-error-classifier.util';

describe('classifyDynamicDatabaseError', () => {
  it('classifies a PostgreSQL incompatible operator by driver code', () => {
    expect(
      classifyDynamicDatabaseError(
        { code: '42883', message: 'arbitrary driver wording' },
        'postgres',
      ),
    ).toEqual({
      backend: 'postgres',
      driver: 'pg',
      code: '42883',
      kind: 'postgres_incompatible_operator',
    });
  });

  it.each([
    ['mysql', { code: '42883' }, 'mysql2'],
    ['mongodb', { code: 42883 }, 'mongodb'],
    ['postgres', { code: '23505' }, 'pg'],
  ] as const)(
    'does not classify unrelated %s driver errors as recoverable',
    (backend, error, driver) => {
      expect(classifyDynamicDatabaseError(error, backend)).toEqual({
        backend,
        driver,
        code: String(error.code),
        kind: 'unknown',
      });
    },
  );

  it('retains an unknown code without falling back to message parsing', () => {
    expect(
      classifyDynamicDatabaseError(
        { message: 'operator does not exist: uuid = character varying' },
        'postgres',
      ),
    ).toEqual({
      backend: 'postgres',
      driver: 'pg',
      code: null,
      kind: 'unknown',
    });
  });
});
