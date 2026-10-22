import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { asValue } from 'awilix';
import type { QueryBuilderService } from '@enfyra/kernel';
import { MongoJunctionHealingService } from '../../../src/engines/bootstrap/services/schema-healing/mongo-junction-healing.service';
import { SqlJunctionHealingService } from '../../../src/engines/bootstrap/services/schema-healing/sql-junction-healing.service';
import { ObjectId } from 'mongodb';
import { buildContainer } from '../../../src/container';
import { bootstrapSourceArtifacts } from '../../../src/data';
import { BootstrapDefinitionService } from '../../../src/engines/bootstrap/services/bootstrap-definition.service';
import { init, shutdown } from '../../../src/init';
import { CodeAuditService } from '../../../src/modules/dynamic-api/services/code-audit.service';
import { getSqlJunctionPhysicalNames } from '../../../src/modules/table-management/utils/sql-junction-naming.util';
import type { TDynamicContext } from '../../../src/shared/types';

async function verifyJunctionConvergence(
  query: QueryBuilderService,
  sourceId: unknown,
  targetId: unknown,
) {
  for (const scenario of [
    'old-only',
    'equal-aliases',
    'conflict',
    'unexpected-field',
  ]) {
    const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
    const canonical = getSqlJunctionPhysicalNames({
      sourceTable: 'enfyra_user',
      propertyName: `fixture_${suffix}`,
      targetTable: 'enfyra_role',
    });
    const previous =
      scenario === 'equal-aliases'
        ? canonical.junctionTableName
        : `fixture_junction_${suffix}`;
    const input = {
      ...canonical,
      sourceTable: 'enfyra_user',
      targetTable: 'enfyra_role',
      propertyName: `fixture_${suffix}`,
      previousTableName: previous,
      previousSourceColumn: 'fixtureUserId',
      previousTargetColumn: 'fixtureRoleId',
    };
    const row: Record<string, unknown> = {
      fixtureUserId: sourceId,
      fixtureRoleId: targetId,
    };
    if (scenario === 'equal-aliases')
      Object.assign(row, { sourceId: null, targetId, fixtureRoleId: null });
    if (scenario === 'conflict')
      Object.assign(row, {
        sourceId: query.isMongoDb() ? new ObjectId() : randomUUID(),
        targetId,
      });
    if (scenario === 'unexpected-field') row.businessData = 'preserve me';
    const rejected = scenario === 'conflict' || scenario === 'unexpected-field';
    if (query.isMongoDb()) {
      const db = query.getMongoDb();
      try {
        await db.collection(previous).insertOne(row);
        const before = await db.collection(previous).find({}).toArray();
        const indexes = await db.collection(previous).listIndexes().toArray();
        const heal = () =>
          new MongoJunctionHealingService().converge(db, input);
        if (rejected) {
          await assert.rejects(heal(), /conflicting|unexpected/);
          assert.equal(
            await db
              .listCollections({ name: canonical.junctionTableName })
              .next(),
            null,
          );
          assert.deepEqual(
            await db.collection(previous).find({}).toArray(),
            before,
          );
          assert.deepEqual(
            await db.collection(previous).listIndexes().toArray(),
            indexes,
          );
        } else {
          await heal();
          await heal();
          if (previous !== canonical.junctionTableName)
            assert.equal(
              await db.listCollections({ name: previous }).next(),
              null,
            );
          const rows = await db
            .collection(canonical.junctionTableName)
            .find({})
            .toArray();
          assert.equal(rows.length, 1);
          assert.deepEqual(Object.keys(rows[0]).sort(), [
            '_id',
            'sourceId',
            'targetId',
          ]);
          assert.equal(String(rows[0].sourceId), String(sourceId));
          assert.equal(String(rows[0].targetId), String(targetId));
        }
      } finally {
        for (const name of new Set([previous, canonical.junctionTableName]))
          if (await db.listCollections({ name }).next())
            await db.collection(name).drop();
      }
    } else {
      const knex = query.getKnex();
      try {
        await knex.schema.createTable(previous, (table) => {
          table.uuid('fixtureUserId');
          table.integer('fixtureRoleId').unsigned();
          if ('sourceId' in row) {
            table.uuid('sourceId');
            table.integer('targetId').unsigned();
          }
          if ('businessData' in row) table.string('businessData');
        });
        await knex(previous).insert(row);
        const before = await knex(previous).select('*');
        const columns = await knex(previous).columnInfo();
        const heal = () =>
          new SqlJunctionHealingService(query).converge(input, async () => {
            throw new Error('Existing fixture storage must be preserved');
          });
        if (rejected) {
          await assert.rejects(heal(), /conflicting|unexpected/);
          assert.equal(
            await knex.schema.hasTable(canonical.junctionTableName),
            false,
          );
          assert.deepEqual(await knex(previous).select('*'), before);
          assert.deepEqual(await knex(previous).columnInfo(), columns);
        } else {
          await heal();
          await heal();
          if (previous !== canonical.junctionTableName)
            assert.equal(await knex.schema.hasTable(previous), false);
          assert.deepEqual(
            Object.keys(
              await knex(canonical.junctionTableName).columnInfo(),
            ).sort(),
            ['sourceId', 'targetId'],
          );
          const rows = await knex(canonical.junctionTableName).select('*');
          assert.equal(rows.length, 1);
          assert.equal(String(rows[0].sourceId), String(sourceId));
          assert.equal(String(rows[0].targetId), String(targetId));
        }
      } finally {
        for (const name of new Set([previous, canonical.junctionTableName]))
          if (await knex.schema.hasTable(name))
            await knex.schema.dropTable(name);
      }
    }
  }
}

async function main() {
  const mode = process.env.AUDIT_DELETE_MODE;
  assert.ok(['fresh', 'source', 'upgrade', 'stable'].includes(mode ?? ''));
  const container = buildContainer();
  const databaseConfigService = container.cradle.databaseConfigService;
  if (mode === 'source') {
    const { enfyra_route_method_config: _auditTarget, ...corrections } =
      bootstrapSourceArtifacts.dataCorrections;
    container.register({
      bootstrapDefinitionService: asValue(
        new BootstrapDefinitionService(
          { databaseConfigService },
          {
            ...bootstrapSourceArtifacts,
            defaultData: {
              ...bootstrapSourceArtifacts.defaultData,
              enfyra_route:
                bootstrapSourceArtifacts.defaultData.enfyra_route.map(
                  (route: Record<string, unknown>) =>
                    route.path === '/enfyra_audit'
                      ? { ...route, availableMethods: ['GET'] }
                      : route,
                ),
            },
            dataCorrections: {
              ...corrections,
              enfyra_route: corrections.enfyra_route.map(
                (route: Record<string, any>) =>
                  route._unique?.path?._eq === '/enfyra_audit'
                    ? { ...route, availableMethods: ['GET'] }
                    : route,
              ),
            },
          },
        ),
      ),
    });
  }
  let bootstrapRuns = 0;
  const initializer = container.cradle.firstRunInitializer;
  const run = initializer.run.bind(initializer);
  initializer.run = async () => {
    bootstrapRuns++;
    await run();
  };
  try {
    await init(container);
    assert.equal(bootstrapRuns, mode === 'stable' ? 0 : 1);
    const query = container.cradle.queryBuilderService;
    const pk = query.getPkField();
    const junction = getSqlJunctionPhysicalNames({
      sourceTable: 'enfyra_user',
      propertyName: 'roles',
      targetTable: 'enfyra_role',
    });
    const owner = await query.findOne({
      table: 'enfyra_user',
      fields: [pk],
      filter: { isRootAdmin: { _eq: true } },
    });
    const role = await query.findOne({
      table: 'enfyra_role',
      fields: [pk],
      filter: { name: { _eq: 'Admin' } },
    });
    assert.ok(owner);
    assert.ok(role);
    const fixtureSourceId = query.isMongoDb()
      ? new ObjectId(String(owner[pk]))
      : owner[pk];
    const fixtureTargetId = query.isMongoDb()
      ? new ObjectId(String(role[pk]))
      : role[pk];
    if (mode === 'fresh')
      await verifyJunctionConvergence(query, fixtureSourceId, fixtureTargetId);
    if (query.isMongoDb()) {
      const db = query.getMongoDb();
      const tables = await db.collection('enfyra_table').find({}).toArray();
      const byId = new Map(
        tables.map((table) => [String(table._id), table.name]),
      );
      const relations = await db
        .collection('enfyra_relation')
        .find({ type: 'many-to-many' })
        .toArray();
      for (const relation of relations.filter((entry) => !entry.mappedBy)) {
        const expected = getSqlJunctionPhysicalNames({
          sourceTable: byId.get(String(relation.sourceTable))!,
          propertyName: relation.propertyName,
          targetTable: byId.get(String(relation.targetTable))!,
        });
        for (const field of [
          'junctionTableName',
          'junctionSourceColumn',
          'junctionTargetColumn',
        ] as const)
          assert.equal(relation[field], expected[field]);
        assert.ok(
          await db.listCollections({ name: expected.junctionTableName }).next(),
        );
        for (const inverse of relations.filter(
          (entry) => String(entry.mappedBy) === String(relation._id),
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
      }
      if (mode === 'source')
        await db
          .collection(junction.junctionTableName)
          .updateOne(
            { sourceId: fixtureSourceId, targetId: fixtureTargetId },
            {
              $setOnInsert: {
                sourceId: fixtureSourceId,
                targetId: fixtureTargetId,
              },
            },
            { upsert: true },
          );
    } else {
      const knex = query.getKnex();
      const relations = await knex('enfyra_relation as relation')
        .join('enfyra_table as source', 'source.id', 'relation.sourceTableId')
        .join('enfyra_table as target', 'target.id', 'relation.targetTableId')
        .where('relation.type', 'many-to-many')
        .select(
          'relation.*',
          'source.name as sourceTableName',
          'target.name as targetTableName',
        );
      for (const relation of relations.filter((entry) => !entry.mappedById)) {
        const expected = getSqlJunctionPhysicalNames({
          sourceTable: relation.sourceTableName,
          propertyName: relation.propertyName,
          targetTable: relation.targetTableName,
        });
        for (const field of [
          'junctionTableName',
          'junctionSourceColumn',
          'junctionTargetColumn',
        ] as const)
          assert.equal(relation[field], expected[field]);
        assert.equal(
          await knex.schema.hasColumn(expected.junctionTableName, 'sourceId'),
          true,
        );
        assert.equal(
          await knex.schema.hasColumn(expected.junctionTableName, 'targetId'),
          true,
        );
        for (const inverse of relations.filter(
          (entry) => String(entry.mappedById) === String(relation.id),
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
      }
      if (
        mode === 'source' &&
        !(await knex(junction.junctionTableName)
          .where({ sourceId: fixtureSourceId, targetId: fixtureTargetId })
          .first())
      )
        await knex(junction.junctionTableName).insert({
          sourceId: fixtureSourceId,
          targetId: fixtureTargetId,
        });
    }
    if (mode === 'upgrade' || mode === 'stable') {
      if (query.isMongoDb())
        assert.equal(
          await query
            .getMongoDb()
            .listCollections({ name: 'fixture_user_roles' })
            .next(),
          null,
        );
      else
        assert.equal(
          await query.getKnex().schema.hasTable('fixture_user_roles'),
          false,
        );
    }
    if (mode !== 'fresh') {
      const hydrated = await query.findOne({
        table: 'enfyra_user',
        fields: [pk, `roles.${pk}`],
        filter: { [pk]: { _eq: owner[pk] } },
      });
      assert.ok(
        hydrated.roles.some(
          (entry: Record<string, unknown>) =>
            String(entry.id ?? entry._id) === String(role[pk]),
        ),
        'Role links must survive upgrade and stable boot',
      );
    }
    const findConfig = (path: string, method: string) =>
      query.findOne({
        table: 'enfyra_route_method_config',
        filter: {
          _and: [
            { route: { path: { _eq: path } } },
            { method: { name: { _eq: method } } },
          ],
        },
        fields: [
          pk,
          'available',
          'timeout',
          'isPublic',
          'skipRoleGuard',
          'handler.id',
        ],
      });
    const deleteConfig = await findConfig('/enfyra_audit', 'DELETE');
    assert.ok(deleteConfig);
    if (mode === 'source') {
      assert.equal(Boolean(deleteConfig.available), false);
      assert.equal(deleteConfig.handler, null);
      const sentinel = await findConfig('/enfyra_helper', 'GET');
      await query.update('enfyra_route_method_config', sentinel[pk], {
        timeout: 45123,
        isPublic: false,
        skipRoleGuard: false,
      });
      const legacy = 'fixture_user_roles';
      const routeJunction = getSqlJunctionPhysicalNames({
        sourceTable: 'enfyra_route',
        propertyName: 'availableMethods',
        targetTable: 'enfyra_method',
      });
      if (query.isMongoDb()) {
        const db = query.getMongoDb();
        const userTable = await db
          .collection('enfyra_table')
          .findOne({ name: 'enfyra_user' });
        const relation = await db
          .collection('enfyra_relation')
          .findOne({ sourceTable: userTable!._id, propertyName: 'roles' });
        await db.collection(junction.junctionTableName).rename(legacy);
        await db
          .collection(legacy)
          .updateMany(
            {},
            {
              $rename: { sourceId: 'fixtureUserId', targetId: 'fixtureRoleId' },
            },
          );
        await db
          .collection('enfyra_relation')
          .updateOne(
            { _id: relation!._id },
            {
              $set: {
                junctionTableName: legacy,
                junctionSourceColumn: 'fixtureUserId',
                junctionTargetColumn: 'fixtureRoleId',
              },
            },
          );
        await db.createCollection(junction.junctionTableName);
        await db
          .collection(junction.junctionTableName)
          .insertOne({ sourceId: fixtureSourceId, targetId: fixtureTargetId });
        await db
          .collection(routeJunction.junctionTableName)
          .dropIndex(`${routeJunction.junctionTableName}_src_tgt_uq`);
        await db
          .collection(routeJunction.junctionTableName)
          .updateMany(
            {},
            {
              $rename: {
                sourceId: 'enfyra_routeId',
                targetId: 'enfyra_methodId',
              },
            },
          );
      } else {
        const knex = query.getKnex();
        const relation = await knex('enfyra_relation as relation')
          .join('enfyra_table as source', 'source.id', 'relation.sourceTableId')
          .where({
            'source.name': 'enfyra_user',
            'relation.propertyName': 'roles',
          })
          .select('relation.id')
          .first();
        await knex.schema.renameTable(junction.junctionTableName, legacy);
        await knex.schema.alterTable(legacy, (table) => {
          table.renameColumn('sourceId', 'fixtureUserId');
          table.renameColumn('targetId', 'fixtureRoleId');
        });
        await knex('enfyra_relation')
          .where({ id: relation.id })
          .update({
            junctionTableName: legacy,
            junctionSourceColumn: 'fixtureUserId',
            junctionTargetColumn: 'fixtureRoleId',
          });
        await knex.schema.createTable(junction.junctionTableName, (table) => {
          table.uuid('sourceId').notNullable();
          table.integer('targetId').unsigned().notNullable();
          table.primary(['sourceId', 'targetId']);
        });
        await knex(junction.junctionTableName).insert({
          sourceId: fixtureSourceId,
          targetId: fixtureTargetId,
        });
        await knex.schema.alterTable(
          routeJunction.junctionTableName,
          (table) => {
            table.renameColumn('sourceId', 'enfyra_routeId');
            table.renameColumn('targetId', 'enfyra_methodId');
          },
        );
      }
      const setting = await query.findOne({
        table: 'enfyra_setting',
        fields: [pk],
      });
      await query.update('enfyra_setting', setting[pk], {
        enfyraVersion: '2.3.2',
      });
    } else {
      assert.equal(Boolean(deleteConfig.available), true);
      assert.ok(deleteConfig.handler?.id ?? deleteConfig.handler?._id);
      const route = await container.cradle.routeCacheService.matchRoute(
        'DELETE',
        '/enfyra_audit/1',
      );
      assert.ok(route, 'DELETE route must be callable');
      assert.equal(
        await container.cradle.routeCacheService.matchRoute(
          'POST',
          '/enfyra_audit',
        ),
        null,
      );
      assert.equal(
        await container.cradle.routeCacheService.matchRoute(
          'PATCH',
          '/enfyra_audit/1',
        ),
        null,
      );
      const audit = new CodeAuditService({ queryBuilderService: query });
      await audit.record({
        tableName: 'enfyra_helper',
        recordId: 'audit-delete-fixture',
        action: 'update',
        fromVersion: '1.0.0',
        toVersion: '1.0.1',
        previous: { sourceCode: 'return 1;' },
      });
      const entry = await query.findOne({
        table: 'enfyra_audit',
        fields: [pk],
        filter: { recordId: { _eq: 'audit-delete-fixture' } },
      });
      assert.ok(entry);
      const context = {
        $user: { isRootAdmin: true },
        $query: {},
        $body: {},
      } as TDynamicContext;
      const repository = container.cradle.dynamicRepositoryFactory.create(
        'enfyra_audit',
        context,
        true,
      );
      await assert.rejects(
        repository.create({
          data: {
            tableName: 'enfyra_helper',
            recordId: 'forged',
            action: 'create',
          },
        }),
        /Code history is read-only/,
      );
      await assert.rejects(
        repository.update({ id: entry[pk], data: { previous: null } }),
        /Code history is read-only/,
      );
      const denied = container.cradle.policyService.checkRequestAccess({
        method: 'DELETE',
        user: { isRootAdmin: false, roles: [] },
        routeData: {
          ...route.route,
          routeMethodConfig: deleteConfig,
          routePermissions: [],
        },
      });
      assert.equal(denied.allow, false);
      await repository.delete({ id: entry[pk] });
      const remaining = await query.find({
        table: 'enfyra_audit',
        fields: [pk],
        filter: { [pk]: { _eq: entry[pk] } },
        limit: 1,
      });
      assert.equal(remaining.data.length, 0);
      if (mode !== 'fresh') {
        const sentinel = await findConfig('/enfyra_helper', 'GET');
        assert.equal(sentinel.timeout, 45123);
        assert.equal(Boolean(sentinel.isPublic), false);
        assert.equal(Boolean(sentinel.skipRoleGuard), false);
      }
    }
    console.log(
      `AUDIT_DELETE_RESULT ${JSON.stringify({ mode, backend: databaseConfigService.getDbType(), bootstrapRuns, passed: true })}`,
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
