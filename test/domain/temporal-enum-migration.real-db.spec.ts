import { randomUUID } from 'node:crypto';
import { knex, type Knex } from 'knex';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { MetadataMigrationService } from '../../src/engines/bootstrap/services/metadata-migration.service';

const uri = process.env.POSTGRES_TEST_URI;

describe.skipIf(!uri)('PostgreSQL temporal enum metadata migration', () => {
  const schema = `temporal_enum_${randomUUID().replaceAll('-', '')}`;
  let admin: Knex;
  let target: Knex;
  let created = false;
  let migrate: (mappings: Array<{ from: string; to: string }>) => Promise<void>;

  beforeAll(async () => {
    admin = knex({ client: 'pg', connection: uri });
    await admin.raw('CREATE SCHEMA ??', [schema]);
    created = true;
    target = knex({ client: 'pg', connection: uri, searchPath: [schema] });
    const service = new MetadataMigrationService({
      queryBuilderService: { getKnex: () => target } as any,
      systemCoreTableResolver: { getNames: async () => ({ column: 'enfyra_column' }) } as any,
    });
    migrate = (mappings) => (service as any).modifySqlColumnTypes(mappings);
  });

  afterAll(async () => {
    if (target) await target.destroy();
    if (admin) {
      if (created) await admin.raw('DROP SCHEMA ?? CASCADE', [schema]);
      await admin.destroy();
    }
  });

  it('rejects a missing target before modifying any source rows', async () => {
    await target.raw("CREATE TYPE type_labels AS ENUM ('date', 'timestamp')");
    await target.raw('CREATE TABLE enfyra_column (type type_labels NOT NULL)');
    await target('enfyra_column').insert([{ type: 'date' }, { type: 'timestamp' }]);
    await expect(migrate([{ from: 'date', to: 'datetime' }])).rejects.toThrow('Column type enum cannot represent datetime');
    expect(await target('enfyra_column').orderBy('type').pluck('type')).toEqual(['date', 'timestamp']);
  });

  it('rewrites present labels and skips a missing source label', async () => {
    await target.raw("ALTER TYPE type_labels ADD VALUE 'datetime'");
    await migrate([{ from: 'date', to: 'datetime' }, { from: 'timestamp', to: 'datetime' }, { from: 'missing', to: 'datetime' }]);
    expect(await target('enfyra_column').orderBy('type').pluck('type')).toEqual(['datetime', 'datetime']);
  });
});
