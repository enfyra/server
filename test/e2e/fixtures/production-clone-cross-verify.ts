import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import knex from 'knex';
import { MongoClient, type ObjectId } from 'mongodb';
import {
  normalizeCloneSqlTimestamp,
  normalizeCloneValue,
} from '../utils/production-clone-value.util';
import { source, batches, mongoRow } from './production-clone-convert';
import { getSqlJunctionPhysicalNames } from '../../../src/modules/table-management/utils/sql-junction-naming.util';
import type {
  CloneCatalog,
  CloneConversionState,
} from '../types/production-clone-conversion.types';

const directory = process.env.CROSS_DB_PRIVATE_DIRECTORY!;
const state: CloneConversionState = JSON.parse(
  readFileSync(join(directory, 'state.json'), 'utf8'),
);
const catalog: CloneCatalog = JSON.parse(
  readFileSync(join(directory, 'source-catalog.json'), 'utf8'),
);
const backend = process.env.CONVERT_BACKEND!;
const beforeUpgrade = process.env.VERIFY_BEFORE_UPGRADE === '1';
const byId = new Map(
  catalog.tables.map((table) => [String(table.id), table.name]),
);
const digest = (row: any) =>
  createHash('sha256')
    .update(JSON.stringify(normalizeCloneValue(row)))
    .digest('hex');
function sqlRow(table: string, row: Record<string, any>) {
  const result: Record<string, any> = {};
  for (const column of catalog.columns.filter(
    (entry) => entry.table_name === table,
  )) {
    const value = row[column.column_name];
    if (value == null) result[column.column_name] = null;
    else if (column.data_type.includes('timestamp'))
      result[column.column_name] = normalizeCloneSqlTimestamp(value);
    else if (column.data_type === 'boolean')
      result[column.column_name] = Boolean(value);
    else if (column.data_type === 'bigint')
      result[column.column_name] = String(value);
    else if (column.data_type === 'real')
      result[column.column_name] = Math.fround(Number(value));
    else if (column.data_type === 'double precision')
      result[column.column_name] = Number(value);
    else result[column.column_name] = value;
  }
  return result;
}
async function main() {
  const database =
    backend === 'mysql' ? state.mysqlDatabase : state.mongoDatabase;
  const mysql =
    backend === 'mysql'
      ? knex({
          client: 'mysql2',
          connection: {
            host: '127.0.0.1',
            port: 3306,
            user: process.env.CLONE_MYSQL_USER,
            password: process.env.CLONE_MYSQL_PASSWORD,
            database,
            timezone: 'Z',
            supportBigNumbers: true,
            bigNumberStrings: true,
            dateStrings: true,
          },
          pool: { min: 0, max: 1 },
        })
      : null;
  const uri = new URL('mongodb://127.0.0.1:27017/admin');
  uri.username = process.env.CLONE_MONGO_USER!;
  uri.password = process.env.CLONE_MONGO_PASSWORD!;
  const client =
    backend === 'mongodb'
      ? new MongoClient(uri.toString(), { promoteLongs: false })
      : null;
  const mismatches: Array<{ table: string; rows: number; reason: string }> = [];
  const business = catalog.tables.filter(
    (table) => !table.isSystem && !table.name.startsWith('enfyra_'),
  );
  const scripts = [
    'enfyra_route_handler',
    'enfyra_pre_hook',
    'enfyra_post_hook',
    'enfyra_helper',
    'enfyra_flow_step',
    'enfyra_websocket',
    'enfyra_websocket_event',
    'enfyra_extension',
  ];
  let businessRows = 0;
  let scriptRows = 0;
  let junctionRows = 0;
  try {
    if (client) await client.connect();
    const db = client?.db(database);
    for (const table of [
      ...business.map((table) => table.name),
      ...scripts,
    ].filter(
      (name) =>
        !process.env.VERIFY_ONLY_TABLE ||
        name === process.env.VERIFY_ONLY_TABLE,
    )) {
      let count = 0;
      const isScript = scripts.includes(table);
      const originalIds: Array<string | number | object> = [];
      const field = table === 'enfyra_extension' ? 'code' : 'sourceCode';
      for await (const rows of batches(table)) {
        const expected =
          backend === 'mysql'
            ? rows.map((row) => sqlRow(table, row))
            : rows.map((row) => mongoRow(table, row));
        if (isScript)
          originalIds.push(...expected.map((row) => row.id ?? row._id));
        const actual = mysql
          ? await mysql(table)
              .whereIn(
                'id',
                rows.map((row) => row.id),
              )
              .select(
                catalog.columns
                  .filter((column) => column.table_name === table)
                  .map((column) =>
                    column.data_type === 'real'
                      ? mysql.raw('CAST(?? AS DOUBLE) AS ??', [
                          column.column_name,
                          column.column_name,
                        ])
                      : column.column_name,
                  ),
              )
          : await db!
              .collection(table)
              .find({ _id: { $in: expected.map((row) => row._id) } })
              .toArray();
        const actualById = new Map(
          actual.map((row: any) => [String(row.id ?? row._id), row]),
        );
        let errors = 0;
        for (const row of expected) {
          const actualRow = actualById.get(String(row.id ?? row._id));
          if (!actualRow) {
            errors++;
            continue;
          }
          if (isScript) {
            if (row[field] !== actualRow[field]) errors++;
          } else {
            const right = mysql ? sqlRow(table, actualRow) : actualRow;
            if (digest(row) !== digest(right)) {
              errors++;
              if (errors === 1)
                console.log(
                  `VERIFY_FIELD_DIFF ${table} ${JSON.stringify(
                    Object.keys(row)
                      .filter((key) => digest(row[key]) !== digest(right[key]))
                      .map((key) => ({
                        field: key,
                        sourceType: typeof row[key],
                        targetType: typeof right[key],
                      })),
                  )}`,
                );
            }
          }
        }
        if (errors)
          mismatches.push({
            table,
            rows: errors,
            reason: isScript
              ? 'existing script source differs'
              : 'converted business row differs',
          });
        count += rows.length;
      }
      if (!isScript) {
        const actualCount = mysql
          ? Number((await mysql(table).count({ count: '*' }))[0].count)
          : await db!.collection(table).countDocuments({});
        if (actualCount !== count)
          mismatches.push({
            table,
            rows: Math.abs(actualCount - count),
            reason: 'business row count differs',
          });
        businessRows += count;
      } else {
        const expectedAddition =
          !beforeUpgrade && table === 'enfyra_route_handler' ? 1 : 0;
        const actualCount = mysql
          ? Number((await mysql(table).count({ count: '*' }))[0].count)
          : await db!.collection(table).countDocuments({});
        assert.equal(
          actualCount,
          count + expectedAddition,
          `Unexpected script additions: ${table}`,
        );
        if (expectedAddition) {
          const route = await source('enfyra_route')
            .where('path', '/enfyra_audit')
            .first();
          const method = await source('enfyra_method')
            .where('name', 'DELETE')
            .first();
          assert.ok(route && method);
          if (mysql) {
            const added = await mysql(table)
              .whereNotIn('id', originalIds as Array<string | number>)
              .select('routeId', 'methodId', 'routeMethodConfigId');
            assert.equal(added.length, 1);
            assert.equal(String(added[0].routeId), String(route.id));
            assert.equal(String(added[0].methodId), String(method.id));
            assert.ok(
              await mysql('enfyra_route_method_config')
                .where({
                  id: added[0].routeMethodConfigId,
                  routeId: route.id,
                  methodId: method.id,
                  available: true,
                })
                .first(),
            );
          } else {
            const added = await db!
              .collection(table)
              .find(
                { _id: { $nin: originalIds as ObjectId[] } },
                { projection: { route: 1, method: 1, routeMethodConfig: 1 } },
              )
              .toArray();
            assert.equal(added.length, 1);
            assert.equal(
              String(added[0].route),
              String(mongoRow('enfyra_route', route)._id),
            );
            assert.equal(
              String(added[0].method),
              String(mongoRow('enfyra_method', method)._id),
            );
            assert.ok(
              await db!.collection('enfyra_route_method_config').findOne({
                _id: added[0].routeMethodConfig,
                route: added[0].route,
                method: added[0].method,
                available: true,
              }),
            );
          }
        }
        scriptRows += count;
      }
      console.log(`VERIFY_PROGRESS ${backend} ${table} ${count}`);
    }
    const junctions = catalog.relations.filter(
      (relation) => relation.type === 'many-to-many' && !relation.mappedById,
    );
    for (const relation of junctions) {
      const canonical = beforeUpgrade
        ? {
            junctionTableName: relation.junctionTableName,
            junctionSourceColumn: relation.junctionSourceColumn,
            junctionTargetColumn: relation.junctionTargetColumn,
          }
        : getSqlJunctionPhysicalNames({
            sourceTable: byId.get(String(relation.sourceTableId)),
            propertyName: relation.propertyName,
            targetTable: byId.get(String(relation.targetTableId)),
          });
      let expectedCount = 0;
      for await (const rows of batches(relation.junctionTableName)) {
        const expected = rows.map((row) => {
          const mapped = mysql
            ? row
            : mongoRow(relation.junctionTableName, row);
          return {
            sourceId: mapped[relation.junctionSourceColumn],
            targetId: mapped[relation.junctionTargetColumn],
          };
        });
        const actual = mysql
          ? await mysql(canonical.junctionTableName)
              .whereIn(
                canonical.junctionSourceColumn,
                expected.map((row) => row.sourceId),
              )
              .select({
                sourceId: canonical.junctionSourceColumn,
                targetId: canonical.junctionTargetColumn,
              })
          : await db!
              .collection(canonical.junctionTableName)
              .find(
                {
                  [canonical.junctionSourceColumn]: {
                    $in: expected.map((row) => row.sourceId),
                  },
                },
                {
                  projection: {
                    _id: 0,
                    [canonical.junctionSourceColumn]: 1,
                    [canonical.junctionTargetColumn]: 1,
                  },
                },
              )
              .toArray()
              .then((rows) =>
                rows.map((row) => ({
                  sourceId: row[canonical.junctionSourceColumn],
                  targetId: row[canonical.junctionTargetColumn],
                })),
              );
        const identities = new Set(
          actual.map((row) =>
            digest({
              sourceId: String(row.sourceId),
              targetId: String(row.targetId),
            }),
          ),
        );
        const missing = expected.filter(
          (row) =>
            !identities.has(
              digest({
                sourceId: String(row.sourceId),
                targetId: String(row.targetId),
              }),
            ),
        ).length;
        if (missing)
          mismatches.push({
            table: relation.junctionTableName,
            rows: missing,
            reason: 'original relation link missing',
          });
        expectedCount += rows.length;
      }
      const actualCount = mysql
        ? Number(
            (await mysql(canonical.junctionTableName).count({ count: '*' }))[0]
              .count,
          )
        : await db!.collection(canonical.junctionTableName).countDocuments({});
      const auditAddition =
        !beforeUpgrade &&
        byId.get(String(relation.sourceTableId)) === 'enfyra_route' &&
        relation.propertyName === 'availableMethods'
          ? 1
          : 0;
      if (actualCount !== expectedCount + auditAddition)
        mismatches.push({
          table: canonical.junctionTableName,
          rows: Math.abs(actualCount - expectedCount - auditAddition),
          reason: 'unexpected relation link count',
        });
      if (auditAddition) {
        const route = await source('enfyra_route')
          .where('path', '/enfyra_audit')
          .first();
        const method = await source('enfyra_method')
          .where('name', 'DELETE')
          .first();
        assert.ok(route && method);
        const edge = mysql
          ? { sourceId: route.id, targetId: method.id }
          : {
              sourceId: mongoRow('enfyra_route', route)._id,
              targetId: mongoRow('enfyra_method', method)._id,
            };
        assert.ok(
          mysql
            ? await mysql(canonical.junctionTableName).where(edge).first()
            : await db!.collection(canonical.junctionTableName).findOne(edge),
        );
      }
      junctionRows += expectedCount;
    }
    const physicalCounts: Record<string, number> = {};
    if (beforeUpgrade) {
      for (const table of new Set(
        catalog.columns.map((column) => column.table_name),
      )) {
        const expected = Number(
          (await source(table).count({ count: '*' }))[0].count,
        );
        const actual = mysql
          ? Number((await mysql(table).count({ count: '*' }))[0].count)
          : await db!.collection(table).countDocuments({});
        assert.equal(
          actual,
          expected,
          `Imported physical table count differs: ${table}`,
        );
        physicalCounts[table] = actual;
      }
    }
    const result = {
      backend,
      phase: beforeUpgrade ? 'before-upgrade' : 'after-upgrade',
      physicalTablesCounted: Object.keys(physicalCounts).length,
      importedRowsCounted: Object.values(physicalCounts).reduce(
        (total, value) => total + value,
        0,
      ),
      businessTables: business.length,
      businessRows,
      scriptTables: scripts.length,
      scriptRows,
      junctions: junctions.length,
      junctionRows,
      mismatches,
      temporalPrecision:
        backend === 'mongodb'
          ? 'BSON milliseconds'
          : 'UTC microseconds compared for every original business row',
    };
    writeFileSync(
      join(
        directory,
        `${backend}-${beforeUpgrade ? 'before-upgrade-' : ''}preservation.json`,
      ),
      JSON.stringify(result, null, 2),
      { mode: 0o600 },
    );
    console.log(`CROSS_VERIFY_RESULT ${JSON.stringify(result)}`);
    assert.equal(
      mismatches.length,
      0,
      'Preservation mismatches; inspect private summary',
    );
  } finally {
    await source.destroy();
    if (mysql) await mysql.destroy();
    if (client) await client.close();
  }
}
void main().catch((error) => {
  writeFileSync(
    join(directory, `${backend}-verification-error.log`),
    String(error.stack ?? error),
    { mode: 0o600 },
  );
  console.error(
    'Cross-database preservation verification failed; private diagnostic retained',
  );
  process.exitCode = 1;
});
