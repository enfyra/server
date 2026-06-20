import { describe, expect, it } from 'vitest';
import knexFactory from 'knex';

describe('SchemaMigrationLockService SQL identifiers', () => {
  it('quotes camelCase lock token column on PostgreSQL updates', () => {
    const knex = knexFactory({ client: 'pg' });

    const sql = knex('schema_migration_lock')
      .where({ id: 1 })
      .where({ lockToken: 'token' })
      .update({ isLocked: false })
      .toSQL().sql;

    expect(sql).toContain('"lockToken"');
    expect(sql).not.toContain('locktoken');
  });
});
