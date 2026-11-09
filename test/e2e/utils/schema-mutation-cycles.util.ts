import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Cradle } from '../../../src/container';
import type { TDynamicContext } from '../../../src/shared/types';
import type { TCreateColumnBody } from '../../../src/modules/table-management/types/table-handler.types';
import type { RuntimeMigrationMatrixDriver } from '../types/runtime-migration-matrix.types';
import { getCurrentDatabaseSchema } from '../../../src/engines/knex/utils/provision/schema-comparison';
import { getPostgresEnumTypeName } from '../../../src/engines/knex/utils/sql-enum.util';

export async function verifySchemaMutationCycles(
  c: Cradle,
  driver: RuntimeMigrationMatrixDriver,
): Promise<string[]> {
  assert.equal(process.env.NODE_ENV, 'test');
  const uri = new URL(process.env.DB_URI!);
  assert.ok(['127.0.0.1', 'localhost'].includes(uri.hostname));
  assert.match(uri.pathname.slice(1), /^enfyra_lifecycle_matrix_[a-f0-9]{32}$/);
  const query = c.queryBuilderService;
  const mongo = query.isMongoDb();
  const pk = query.getPkField();
  const name = `schema_cycle_${randomUUID().replaceAll('-', '').slice(0, 10)}`;
  const context = {
    $user: { isRootAdmin: true, roles: [] },
    $query: {},
    $body: {},
  } as TDynamicContext;
  const repo = (table: string) =>
    c.dynamicRepositoryFactory.create(table, context);
  const primary: TCreateColumnBody = {
    name: pk,
    type: mongo ? 'objectId' : 'int',
    isPrimary: true,
    isGenerated: true,
    isNullable: false,
  };
  const status: TCreateColumnBody = {
    name: 'status',
    type: 'enum',
    options: ['raw', 'bearer'],
    isNullable: false,
    defaultValue: 'raw',
  };
  let tableId: string | number | undefined;
  const checks: string[] = [];
  const serializable = (value: unknown) => JSON.parse(JSON.stringify(value));

  async function create() {
    const result = await repo('enfyra_table').create({
      data: {
        name,
        validateBody: true,
        columns: [
          primary,
          {
            name: 'marker',
            type: mongo ? 'string' : 'varchar',
            isNullable: false,
          },
          { name: 'counter', type: 'int', isNullable: false, defaultValue: 3 },
          status,
        ],
      },
    });
    tableId = result.data[0][pk] ?? result.data[0].id;
    assert.ok(tableId, 'cycle table identity missing');
  }

  async function physical() {
    if (!mongo)
      return serializable(
        await getCurrentDatabaseSchema(query.getKnex(), name),
      );
    const db = query.getMongoDb();
    const [collections, indexes] = await Promise.all([
      db.listCollections({ name }, { nameOnly: false }).toArray(),
      db.collection(name).listIndexes().toArray(),
    ]);
    return serializable({
      options: collections[0]?.options,
      indexes: indexes.sort((a, b) =>
        String(a.name).localeCompare(String(b.name)),
      ),
    });
  }

  async function snapshot() {
    return serializable({
      metadata: await driver.loadTableBody(tableId!),
      physical: await physical(),
      rows: (
        await repo(name).find({
          fields: [pk, 'marker', 'counter', 'status'],
          sort: ['marker'],
          limit: -1,
        })
      ).data,
    });
  }

  async function assertEnum(expected: string[]) {
    const body = await driver.loadTableBody(tableId!);
    assert.deepEqual(
      body.columns.find((column) => column.name === 'status')?.options,
      expected,
    );
    if (mongo) {
      const db = query.getMongoDb();
      const collection = (
        await db.listCollections({ name }, { nameOnly: false }).toArray()
      )[0];
      assert.equal(
        collection?.options?.validator?.$jsonSchema?.properties?.status
          ?.bsonType,
        'string',
        'MongoDB enum storage must retain the canonical BSON string validator',
      );
    } else {
      const schema = await getCurrentDatabaseSchema(query.getKnex(), name);
      const column = schema.columns.find((value) => value.name === 'status');
      assert.equal(
        column?.type.toLowerCase().startsWith('enum'),
        true,
        'enum column must stay native',
      );
      assert.deepEqual(column?.enumValues, expected);
    }
  }

  try {
    await create();
    const a = (
      await repo(name).create({
        data: { marker: 'a', counter: 13, status: 'raw' },
      })
    ).data[0];
    const b = (
      await repo(name).create({
        data: { marker: 'b', counter: 17, status: 'bearer' },
      })
    ).data[0];
    const identities = [String(a[pk]), String(b[pk])];
    for (let cycle = 0; cycle < 2; cycle++) {
      const beforeNoop = await snapshot();
      await driver.applyConfirmedTableUpdate(
        tableId!,
        await driver.loadTableBody(tableId!),
      );
      assert.deepEqual(
        await snapshot(),
        beforeNoop,
        'identical schema submission must be a physical and data no-op',
      );
      await assertEnum(['raw', 'bearer']);

      const beforeFailure = await snapshot();
      const invalid = await driver.loadTableBody(tableId!);
      invalid.columns.push({
        name: `uncommitted_${cycle}`,
        type: 'enum',
        options: ['pending', 'done'],
        isNullable: true,
      });
      invalid.columns.find((column) => column.name === 'status')!.options = [
        'raw',
      ];
      invalid.columns.find(
        (column) => column.name === 'counter',
      )!.defaultValue = 99;
      await assert.rejects(() =>
        driver.applyConfirmedTableUpdate(tableId!, invalid),
      );
      assert.deepEqual(
        await snapshot(),
        beforeFailure,
        'failed multi-change schema must leave metadata, physical contract and rows unchanged',
      );
      if (!mongo && query.getDatabaseType() === 'postgres') {
        const result = await query
          .getKnex()
          .raw('SELECT to_regtype(?) AS type', [
            getPostgresEnumTypeName(name, `uncommitted_${cycle}`),
          ]);
        assert.equal(
          result.rows[0].type,
          null,
          'failed schema must not leak newly planned enum types',
        );
      }

      const drop = await driver.loadTableBody(tableId!);
      drop.columns = drop.columns.filter((column) => column.name !== 'status');
      await driver.applyConfirmedTableUpdate(tableId!, drop);
      assert.equal(
        (await driver.loadTableBody(tableId!)).columns.some(
          (column) => column.name === 'status',
        ),
        false,
      );
      if (mongo) {
        assert.equal(
          await query
            .getMongoDb()
            .collection(name)
            .countDocuments({ status: { $exists: true } }),
          0,
        );
      } else {
        assert.equal(
          await query.getKnex().schema.hasColumn(name, 'status'),
          false,
        );
      }
      const add = await driver.loadTableBody(tableId!);
      add.columns.push({
        ...status,
        defaultValue: cycle === 0 ? 'raw' : 'bearer',
      });
      await driver.applyConfirmedTableUpdate(tableId!, add);
      await assertEnum(['raw', 'bearer']);
      const rows = (
        await repo(name).find({
          fields: [pk, 'marker', 'counter', 'status'],
          sort: ['marker'],
          limit: -1,
        })
      ).data;
      assert.deepEqual(
        rows.map((row) => String(row[pk])),
        identities,
      );
      assert.deepEqual(
        rows.map((row) => Number(row.counter)),
        [13, 17],
      );
      const defaultValue = cycle === 0 ? 'raw' : 'bearer';
      assert.deepEqual(
        rows.map((row) => row.status),
        mongo ? [undefined, undefined] : [defaultValue, defaultValue],
      );
      const defaulted = (
        await repo(name).create({
          data: { marker: `default-${cycle}`, counter: 29 },
        })
      ).data[0];
      const readDefault = (
        await repo(name).find({
          fields: ['status'],
          filter: { [pk]: { _eq: defaulted[pk] } },
        })
      ).data[0];
      assert.equal(
        readDefault.status,
        defaultValue,
        'new rows must use the re-added enum default',
      );
      await repo(name).delete({ id: defaulted[pk] });
      await repo(name).update({ id: b[pk], data: { status: 'bearer' } });
      await repo(name).update({ id: a[pk], data: { status: 'raw' } });
      await assert.rejects(async () => {
        const data = { status: 'unknown' };
        await c.mutationBodyValidationService.validate({
          tableName: name,
          mode: 'update',
          body: data,
          user: context.$user,
        });
        await repo(name).update({ id: a[pk], data });
      });
      const beforeInvalidWrite = await snapshot();
      if (mongo) {
        await assert.rejects(() =>
          query
            .getMongoDb()
            .collection(name)
            .updateOne({ marker: 'a' }, { $set: { status: 123 } }),
        );
      } else {
        await assert.rejects(() =>
          query
            .getKnex()(name)
            .where({ marker: 'a' })
            .update({ status: 'unknown' }),
        );
      }
      assert.deepEqual(
        await snapshot(),
        beforeInvalidWrite,
        'physical enum validation must reject invalid values without mutating rows',
      );
    }
    const beforeFault = await snapshot();
    const attestor = c.runtimeSchemaTargetAttestorService;
    const assertTarget = attestor.assertTarget;
    let injected = 0;
    attestor.assertTarget = async (contract) => {
      await assertTarget.call(attestor, contract);
      if (
        contract.context.target?.name === name &&
        contract.context.operation === 'update'
      ) {
        injected++;
        if (mongo)
          await query
            .getMongoDb()
            .collection(name)
            .updateOne({ marker: 'a' }, { $set: { counter: 999 } });
        else
          await query
            .getKnex()(name)
            .where({ marker: 'a' })
            .update({ counter: 999 });
        throw new Error('injected failure after physical target attestation');
      }
    };
    try {
      const valid = await driver.loadTableBody(tableId!);
      valid.columns.push({
        name: 'attested_only',
        type: 'enum',
        options: ['pending', 'done'],
        isNullable: true,
      });
      valid.columns.find((column) => column.name === 'counter')!.defaultValue =
        77;
      await assert.rejects(() =>
        driver.applyConfirmedTableUpdate(tableId!, valid),
      );
      assert.equal(
        injected,
        1,
        'fault must occur after real physical and metadata execution',
      );
    } finally {
      attestor.assertTarget = assertTarget;
    }
    assert.deepEqual(
      await snapshot(),
      beforeFault,
      'post-DDL failure must restore physical schema, metadata and modified rows',
    );
    checks.push(
      'schema-post-ddl-injected-failure-restores-schema-metadata-and-data',
    );

    for (const operation of ['rename', 'delete'] as const) {
      const before = await snapshot();
      const renamed = `${name}_renamed`;
      let reached = 0;
      attestor.assertTarget = async (contract) => {
        await assertTarget.call(attestor, contract);
        if (
          contract.context.source?.name === name &&
          contract.context.operation ===
            (operation === 'rename' ? 'update' : 'delete')
        ) {
          reached++;
          throw new Error(`injected failure after table ${operation}`);
        }
      };
      try {
        if (operation === 'rename') {
          const body = await driver.loadTableBody(tableId!);
          body.name = renamed;
          await assert.rejects(() =>
            driver.applyConfirmedTableUpdate(tableId!, body),
          );
        } else {
          await assert.rejects(() => driver.deleteTableConfirmed(tableId!));
        }
        assert.equal(
          reached,
          1,
          `${operation} fault must follow actual target attestation`,
        );
      } finally {
        attestor.assertTarget = assertTarget;
      }
      assert.deepEqual(
        await snapshot(),
        before,
        `failed table ${operation} must restore the source completely`,
      );
      if (mongo)
        assert.equal(
          (
            await query
              .getMongoDb()
              .listCollections({ name: renamed })
              .toArray()
          ).length,
          0,
        );
      else
        assert.equal(
          await query.getKnex().schema.hasTable(renamed),
          false,
          'failed rename must not leave a second business table',
        );
      checks.push(
        `schema-post-ddl-table-${operation}-rollback-preserves-source`,
      );
    }
    checks.push('schema-two-cycle-noop-and-mixed-failure-atomicity');
    checks.push(
      'schema-enum-drop-readd-defaults-native-validation-and-row-preservation',
    );

    for (let cycle = 0; cycle < 2; cycle++) {
      await driver.deleteTableConfirmed(tableId!);
      tableId = undefined;
      assert.equal(
        await query.findOne({
          table: 'enfyra_table',
          filter: { name: { _eq: name } },
          fields: [pk],
        }),
        null,
      );
      if (mongo)
        assert.equal(
          (await query.getMongoDb().listCollections({ name }).toArray()).length,
          0,
        );
      else assert.equal(await query.getKnex().schema.hasTable(name), false);
      await create();
      await assertEnum(['raw', 'bearer']);
      assert.equal(
        (await repo(name).find({ fields: [pk], limit: -1 })).data.length,
        0,
      );
      await repo(name).create({
        data: { marker: `recreated-${cycle}`, counter: 23, status: 'bearer' },
      });
      const rows = (
        await repo(name).find({
          fields: ['marker', 'counter', 'status'],
          limit: -1,
        })
      ).data;
      assert.deepEqual(
        rows.map((row) => [row.marker, Number(row.counter), row.status]),
        [[`recreated-${cycle}`, 23, 'bearer']],
      );
    }
    checks.push('schema-two-cycle-table-delete-recreate-same-enum-identity');
    return checks;
  } finally {
    if (tableId !== undefined) await driver.deleteTableConfirmed(tableId);
  }
}
