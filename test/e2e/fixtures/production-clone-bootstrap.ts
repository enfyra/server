import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import createKnex from 'knex';
import { MongoClient } from 'mongodb';
import { buildContainer } from '../../../src/container';
import { initBootstrap, shutdown } from '../../../src/init';
import { phaseCoreCaches } from '../../../src/wiring/init/phases';
import { getEnfyraVersion } from '../../../src/shared/utils/enfyra-version.util';
import { getSqlJunctionPhysicalNames } from '../../../src/modules/table-management/utils/sql-junction-naming.util';
import { verifyCloneRuntime } from '../utils/production-clone-runtime.util';

async function main() {
  const uri = new URL(process.env.DB_URI!);
  assert.ok(['localhost', '127.0.0.1'].includes(uri.hostname));
  assert.match(uri.pathname.slice(1), /^enfyra_1endpoint_clone_[a-f0-9]{32}$/);
  const directory = process.env.CLONE_PRIVATE_DIRECTORY;
  assert.ok(directory);
  if (process.env.CLONE_FORCE_BOOTSTRAP === '1') {
    if (uri.protocol === 'mongodb:') {
      const client = new MongoClient(uri.toString());
      try {
        await client.connect();
        await client
          .db(uri.pathname.slice(1))
          .collection('enfyra_setting')
          .updateMany({}, { $set: { isInit: false } });
      } finally {
        await client.close();
      }
    } else {
      const db = createKnex({
        client: uri.protocol === 'mysql:' ? 'mysql2' : 'pg',
        connection: uri.toString(),
        pool: { min: 0, max: 1 },
      });
      try {
        await db('enfyra_setting').update({ isInit: false });
      } finally {
        await db.destroy();
      }
    }
  }
  const container = buildContainer();
  let bootstrapRuns = 0;
  const initializer = container.cradle.firstRunInitializer;
  const run = initializer.run.bind(initializer);
  initializer.run = async () => {
    bootstrapRuns++;
    await run();
  };
  try {
    await initBootstrap(container);
    assert.equal(
      bootstrapRuns,
      Number(process.env.CLONE_EXPECT_BOOTSTRAP_RUNS ?? '1'),
    );
    const query = container.cradle.queryBuilderService;
    const mongo = query.isMongoDb();
    const backend = container.cradle.databaseConfigService.getDbType();
    const db = mongo ? query.getMongoDb() : null;
    const knex = mongo ? null : query.getKnex();
    const settings = mongo
      ? await db!.collection('enfyra_setting').findOne({})
      : await knex!('enfyra_setting').select('isInit', 'enfyraVersion').first();
    assert.ok(settings);
    assert.equal(Boolean(settings.isInit), true);
    assert.equal(settings.enfyraVersion, getEnfyraVersion());
    if (bootstrapRuns === 0) {
      container.cradle.bootstrapDefinitionService.resolveForVersion(
        settings.enfyraVersion,
      );
      await container.cradle.metadataMigrationService.prepareMigrationExecutionPlan();
    }
    await container.cradle.snapshotTargetVerifierService.assertSchemaTargetState();
    const tables = mongo
      ? await db!.collection('enfyra_table').find({}).toArray()
      : await knex!('enfyra_table').select('*');
    const byId = new Map(
      tables.map((table: any) => [String(table.id ?? table._id), table.name]),
    );
    const relations = mongo
      ? await db!
          .collection('enfyra_relation')
          .find({ type: 'many-to-many' })
          .toArray()
      : await knex!('enfyra_relation')
          .where('type', 'many-to-many')
          .select('*');
    let owningJunctions = 0;
    for (const relation of relations.filter(
      (entry: any) => !(entry.mappedById ?? entry.mappedBy),
    )) {
      const expected = getSqlJunctionPhysicalNames({
        sourceTable: byId.get(
          String(relation.sourceTableId ?? relation.sourceTable),
        ) as string,
        propertyName: relation.propertyName,
        targetTable: byId.get(
          String(relation.targetTableId ?? relation.targetTable),
        ) as string,
      });
      for (const field of [
        'junctionTableName',
        'junctionSourceColumn',
        'junctionTargetColumn',
      ] as const)
        assert.equal(relation[field], expected[field]);
      if (mongo) {
        assert.ok(
          await db!
            .listCollections({ name: expected.junctionTableName })
            .next(),
        );
        assert.equal(
          await db!.collection(expected.junctionTableName).countDocuments({
            $or: [
              { sourceId: { $exists: false } },
              { targetId: { $exists: false } },
            ],
          }),
          0,
        );
      } else
        assert.deepEqual(
          Object.keys(
            await knex!(expected.junctionTableName).columnInfo(),
          ).sort(),
          ['sourceId', 'targetId'],
        );
      for (const inverse of relations.filter(
        (entry: any) =>
          String(entry.mappedById ?? entry.mappedBy) ===
          String(relation.id ?? relation._id),
      )) {
        assert.equal(inverse.junctionTableName, expected.junctionTableName);
        assert.equal(
          inverse.junctionSourceColumn,
          expected.junctionTargetColumn,
        );
        assert.equal(
          inverse.junctionTargetColumn,
          expected.junctionSourceColumn,
        );
      }
      owningJunctions++;
    }
    await phaseCoreCaches(container.cradle);
    await container.cradle.runtimeRegistryService.publishFromCache(
      'metadata',
      container.cradle.metadataCacheService,
    );
    const pk = query.getPkField();
    const roleUser = await query.findOne({
      table: 'enfyra_user',
      fields: [pk, `roles.${pk}`],
      filter: { isRootAdmin: { _eq: true } },
    });
    assert.ok(
      roleUser && Array.isArray(roleUser.roles) && roleUser.roles.length > 0,
      'Retained user roles must hydrate through QueryBuilder',
    );
    if (process.env.CLONE_RUN_PROBES === '1') {
      for (const [identifier, service] of [
        ['field-permission', container.cradle.fieldPermissionCacheBuilder],
        ['column-rule', container.cradle.columnRuleCacheBuilder],
        ['setting', container.cradle.settingCacheService],
      ] as const) {
        await service.reload();
        await container.cradle.runtimeRegistryService.publishFromCache(
          identifier,
          service,
        );
      }
    }
    const runtimeChecks =
      process.env.CLONE_RUN_PROBES === '1'
        ? await verifyCloneRuntime(container.cradle)
        : [];
    const counts: Record<string, string> = {};
    for (const table of tables) {
      if (mongo)
        counts[table.name] = String(
          await db!.collection(table.name).countDocuments({}),
        );
      else {
        const [row] = await knex!(table.name).count({ count: '*' });
        counts[table.name] = String(row.count);
      }
    }
    const scripts: Record<string, string> = {};
    for (const table of [
      'enfyra_route_handler',
      'enfyra_pre_hook',
      'enfyra_post_hook',
      'enfyra_helper',
      'enfyra_flow_step',
      'enfyra_websocket',
      'enfyra_websocket_event',
      'enfyra_extension',
    ]) {
      const field = table === 'enfyra_extension' ? 'code' : 'sourceCode';
      const rows = mongo
        ? await db!
            .collection(table)
            .find({}, { projection: { _id: 1, [field]: 1 } })
            .sort({ _id: 1 })
            .toArray()
        : await knex!(table).select('id', field).orderBy('id');
      scripts[table] = createHash('sha256')
        .update(JSON.stringify(rows))
        .digest('hex');
    }
    const result = {
      version: settings.enfyraVersion,
      backend,
      bootstrapRuns,
      tableCount: tables.length,
      owningJunctions,
      roleHydrationPassed: true,
      runtimeChecks,
      counts,
      scripts,
    };
    writeFileSync(
      join(
        directory,
        `verification-${backend}-${bootstrapRuns ? 'upgrade' : 'stable'}.json`,
      ),
      JSON.stringify(result, null, 2),
      { mode: 0o600 },
    );
    console.log(
      `CLONE_RESULT ${JSON.stringify({ version: settings.enfyraVersion, backend, bootstrapRuns, tableCount: tables.length, owningJunctions, roleHydrationPassed: true, attestationPassed: true, runtimeChecks, automationStarted: false })}`,
    );
  } finally {
    await shutdown(container);
  }
}
void main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
