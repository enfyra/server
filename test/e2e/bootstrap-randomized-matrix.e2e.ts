import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import path from 'node:path';
import knex, { type Knex } from 'knex';
import { MongoClient, type Db } from 'mongodb';
import {
  assertBootstrapMatrixDatabaseName,
  createBootstrapMatrixDatabaseName,
  deriveBootstrapMatrixCaseSeed,
  resolveBootstrapMatrixConfig,
} from './bootstrap-randomized-matrix.config';
import type {
  BootstrapMatrixDatabase,
  BootstrapMatrixPhysicalIndex,
  RandomizedBootstrapScenario,
} from './types/bootstrap-randomized-matrix.types';

const CHILD_MODE = 'BOOTSTRAP_MATRIX_CHILD';
const CHILD_SEED = 'BOOTSTRAP_MATRIX_CASE_SEED';

type MatrixConnection =
  | { database: 'postgres' | 'mysql'; sql: Knex }
  | { database: 'mongodb'; mongo: Db; client: MongoClient };

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function sqlAdminConnection(database: 'postgres' | 'mysql'): Knex.Config {
  const prefix = database === 'postgres' ? 'MATRIX_POSTGRES' : 'MATRIX_MYSQL';
  return {
    client: database === 'postgres' ? 'pg' : 'mysql2',
    connection: {
      host: process.env[`${prefix}_HOST`] || '127.0.0.1',
      port: Number(
        process.env[`${prefix}_PORT`] ||
          (database === 'postgres' ? 5432 : 3306),
      ),
      user: required(`${prefix}_USER`),
      password: required(`${prefix}_PASSWORD`),
      database:
        process.env[`${prefix}_DATABASE`] ||
        (database === 'postgres' ? 'enfyra' : 'enfyra_matrix'),
    },
  };
}

function sqlDatabaseUri(
  database: 'postgres' | 'mysql',
  databaseName: string,
): string {
  const connection = sqlAdminConnection(database)
    .connection as Knex.StaticConnectionConfig;
  const protocol = database === 'postgres' ? 'postgres' : 'mysql';
  return `${protocol}://${encodeURIComponent(String(connection.user))}:${encodeURIComponent(
    String(connection.password),
  )}@${connection.host}:${connection.port}/${databaseName}`;
}

function mongoAdminUri(): string {
  const user = encodeURIComponent(required('MATRIX_MONGO_USER'));
  const password = encodeURIComponent(required('MATRIX_MONGO_PASSWORD'));
  const host = process.env.MATRIX_MONGO_HOST || '127.0.0.1';
  const port = Number(process.env.MATRIX_MONGO_PORT || 27017);
  const authDatabase = encodeURIComponent(
    process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin',
  );
  return `mongodb://${user}:${password}@${host}:${port}/?authSource=${authDatabase}`;
}

function mongoDatabaseUri(databaseName: string): string {
  const uri = new URL(mongoAdminUri());
  uri.pathname = `/${databaseName}`;
  return uri.toString();
}

function replayCommand(
  database: BootstrapMatrixDatabase,
  seed: number,
): string {
  return `MATRIX_DATABASES=${database} BOOTSTRAP_MATRIX_SEED=${seed} BOOTSTRAP_MATRIX_CASES=1 yarn test:e2e:bootstrap-randomized-matrix`;
}

async function runChild(
  database: BootstrapMatrixDatabase,
  seed: number,
  databaseUri: string,
): Promise<void> {
  const script = path.resolve(process.argv[1]);
  const child = spawn('yarn', ['tsx', script], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      [CHILD_MODE]: '1',
      [CHILD_SEED]: String(seed),
      MATRIX_DATABASES: database,
      DB_URI: databaseUri,
      NODE_ENV: 'test',
      NODE_NAME: `enfyra-bootstrap-matrix-${database}-${seed >>> 0}`,
      INSTANCE_ID: `bootstrap-matrix-${database}-${seed >>> 0}`,
      REDIS_RUNTIME_CACHE: 'false',
      NODE_OPTIONS: `--no-node-snapshot${
        process.env.NODE_OPTIONS ? ` ${process.env.NODE_OPTIONS}` : ''
      }`,
    },
    stdio: 'inherit',
  });

  await new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      if (code === 0) return resolve();
      reject(
        new Error(
          `Bootstrap matrix child failed for ${database} seed=${seed} (${signal || `exit ${code}`})`,
        ),
      );
    });
  });
}

async function withDisposableDatabase(
  database: BootstrapMatrixDatabase,
  seed: number,
  callback: (databaseUri: string) => Promise<void>,
): Promise<void> {
  const databaseName = createBootstrapMatrixDatabaseName(database, seed);
  assertBootstrapMatrixDatabaseName(databaseName);
  let primaryError: unknown;
  let cleanupError: unknown;

  if (database === 'mongodb') {
    const client = new MongoClient(mongoAdminUri());
    try {
      await client.connect();
      await callback(mongoDatabaseUri(databaseName));
    } catch (error) {
      primaryError = error;
    } finally {
      try {
        assertBootstrapMatrixDatabaseName(databaseName);
        await client.db(databaseName).dropDatabase();
      } catch (error) {
        cleanupError = error;
      }
      await client.close().catch((error) => {
        cleanupError ??= error;
      });
    }
  } else {
    const admin = knex(sqlAdminConnection(database));
    let created = false;
    try {
      await admin.raw('CREATE DATABASE ??', [databaseName]);
      created = true;
      await callback(sqlDatabaseUri(database, databaseName));
    } catch (error) {
      primaryError = error;
    } finally {
      if (created) {
        try {
          assertBootstrapMatrixDatabaseName(databaseName);
          if (database === 'postgres') {
            await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
              databaseName,
            ]);
          } else {
            await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
          }
        } catch (error) {
          cleanupError = error;
        }
      }
      await admin.destroy().catch((error) => {
        cleanupError ??= error;
      });
    }
  }

  if (primaryError && cleanupError) {
    throw new AggregateError(
      [primaryError, cleanupError],
      `Bootstrap matrix failed and cleanup also failed: ${(primaryError as Error).message}`,
    );
  }
  if (primaryError) throw primaryError;
  if (cleanupError) throw cleanupError;
}

async function runCoordinator(): Promise<void> {
  const config = resolveBootstrapMatrixConfig();
  console.log(
    `[bootstrap-matrix] replay-seed=${config.seed} cases=${config.cases} databases=${config.databases.join(',')}`,
  );

  for (const database of config.databases) {
    for (let caseIndex = 0; caseIndex < config.cases; caseIndex++) {
      const seed = deriveBootstrapMatrixCaseSeed(config.seed, caseIndex);
      console.log(
        `[bootstrap-matrix] database=${database} case=${caseIndex + 1}/${config.cases} seed=${seed} source=synthetic-${seed}-source target=synthetic-${seed}-target`,
      );
      try {
        await withDisposableDatabase(database, seed, (databaseUri) =>
          runChild(database, seed, databaseUri),
        );
      } catch (error) {
        console.error(
          `[bootstrap-matrix] replay: ${replayCommand(database, seed)}`,
        );
        throw error;
      }
    }
  }
}

async function openMatrixConnection(
  database: BootstrapMatrixDatabase,
): Promise<MatrixConnection> {
  if (database === 'mongodb') {
    const client = new MongoClient(required('DB_URI'));
    await client.connect();
    return { database, mongo: client.db(), client };
  }
  const sql = knex({
    client: database === 'postgres' ? 'pg' : 'mysql2',
    connection: required('DB_URI'),
  });
  await sql.raw('SELECT 1');
  return { database, sql };
}

async function closeMatrixConnection(
  connection: MatrixConnection,
): Promise<void> {
  if (connection.database === 'mongodb') {
    await connection.client.close();
  } else {
    await connection.sql.destroy();
  }
}

function recordId(seed: number, offset: number): number {
  return 1_000_000 + ((seed >>> 0) % 1_000_000) * 10 + offset;
}

async function setBootstrapRequired(
  connection: MatrixConnection,
): Promise<void> {
  if (connection.database === 'mongodb') {
    const result = await connection.mongo
      .collection('enfyra_setting')
      .updateMany({}, { $set: { isInit: false } });
    assert.ok(result.matchedCount > 0, 'Expected an Enfyra setting document');
    return;
  }
  const updated = await connection
    .sql('enfyra_setting')
    .update({ isInit: false });
  assert.ok(Number(updated) > 0, 'Expected an Enfyra setting row');
}

async function assertInitialized(connection: MatrixConnection): Promise<void> {
  const setting =
    connection.database === 'mongodb'
      ? await connection.mongo.collection('enfyra_setting').findOne({})
      : await connection.sql('enfyra_setting').orderBy('id', 'asc').first();
  assert.ok(setting, 'Expected Enfyra setting metadata');
  assert.ok(
    setting.isInit === true || setting.isInit === 1,
    'Expected bootstrap to mark the database initialized',
  );
}

async function tableExists(
  connection: MatrixConnection,
  table: string,
): Promise<boolean> {
  if (connection.database === 'mongodb') {
    return (
      (await connection.mongo.listCollections({ name: table }).toArray())
        .length > 0
    );
  }
  return connection.sql.schema.hasTable(table);
}

async function columnExists(
  connection: MatrixConnection,
  table: string,
  column: string,
): Promise<boolean> {
  if (connection.database === 'mongodb') {
    return (
      (await connection.mongo
        .collection(table)
        .findOne({ [column]: { $exists: true } })) !== null
    );
  }
  return connection.sql.schema.hasColumn(table, column);
}

async function insertSentinels(
  connection: MatrixConnection,
  scenario: RandomizedBootstrapScenario,
): Promise<void> {
  const { assertions } = scenario;
  const parent = {
    [assertions.renamedColumn.from]: assertions.sentinel.parentLabel,
    [assertions.modifiedColumn]: assertions.sentinel.parentCounter,
    [assertions.removedParentColumn]: 'remove-after-upgrade',
  };
  const child = {
    payload: assertions.sentinel.childPayload,
    [assertions.removedChildColumn]: true,
  };
  const retired = { value: assertions.sentinel.retiredValue };

  if (connection.database === 'mongodb') {
    await connection.mongo
      .collection(assertions.sourceParentTable)
      .insertOne({ _id: recordId(scenario.seed, 1) as never, ...parent });
    await connection.mongo
      .collection(assertions.childTable)
      .insertOne({ _id: recordId(scenario.seed, 2) as never, ...child });
    await connection.mongo
      .collection(assertions.droppedTable)
      .insertOne({ _id: recordId(scenario.seed, 3) as never, ...retired });
    return;
  }

  await connection.sql(assertions.sourceParentTable).insert({
    id: recordId(scenario.seed, 1),
    ...parent,
  });
  await connection.sql(assertions.childTable).insert({
    id: recordId(scenario.seed, 2),
    ...child,
  });
  await connection.sql(assertions.droppedTable).insert({
    id: recordId(scenario.seed, 3),
    ...retired,
  });
}

async function findRecord(
  connection: MatrixConnection,
  table: string,
  id: number,
): Promise<Record<string, unknown> | null> {
  if (connection.database === 'mongodb') {
    return connection.mongo.collection(table).findOne({ _id: id as never });
  }
  return (await connection.sql(table).where({ id }).first()) ?? null;
}

function normalizeJson(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

async function findTableMetadata(
  connection: MatrixConnection,
  name: string,
): Promise<Record<string, any>> {
  const table =
    connection.database === 'mongodb'
      ? await connection.mongo.collection('enfyra_table').findOne({ name })
      : await connection.sql('enfyra_table').where({ name }).first();
  assert.ok(table, `Expected metadata for table ${name}`);
  return table;
}

async function findColumnMetadata(
  connection: MatrixConnection,
  tableName: string,
  columnName: string,
): Promise<Record<string, any> | null> {
  const table = await findTableMetadata(connection, tableName);
  if (connection.database === 'mongodb') {
    return connection.mongo.collection('enfyra_column').findOne({
      table: table._id,
      name: columnName,
    });
  }
  return (
    (await connection
      .sql('enfyra_column')
      .where({
        tableId: table.id,
        name: columnName,
      })
      .first()) ?? null
  );
}

async function relationNames(
  connection: MatrixConnection,
  tableName: string,
): Promise<string[]> {
  const table = await findTableMetadata(connection, tableName);
  const rows =
    connection.database === 'mongodb'
      ? await connection.mongo
          .collection('enfyra_relation')
          .find({ sourceTable: table._id })
          .toArray()
      : await connection
          .sql('enfyra_relation')
          .where({ sourceTableId: table.id });
  return rows.map((row) => String(row.propertyName)).sort();
}

function parseIndexColumns(indexDefinition: string): string[] {
  const match = indexDefinition.match(/\(([^)]+)\)/);
  if (!match) return [];
  return match[1]
    .split(',')
    .map((column) => column.trim().replace(/["`]/g, '').split(/\s+/)[0]);
}

async function physicalIndexes(
  connection: MatrixConnection,
  table: string,
): Promise<BootstrapMatrixPhysicalIndex[]> {
  if (connection.database === 'mongodb') {
    return (
      await connection.mongo.collection(table).listIndexes().toArray()
    ).map((index) => ({
      name: index.name || '',
      columns: Object.keys(index.key),
      unique: index.unique === true,
    }));
  }
  if (connection.database === 'postgres') {
    const result = await connection.sql.raw(
      'SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = ?',
      [table],
    );
    return result.rows.map((row: { indexname: string; indexdef: string }) => ({
      name: row.indexname,
      columns: parseIndexColumns(row.indexdef),
      unique: /CREATE UNIQUE INDEX/i.test(row.indexdef),
    }));
  }
  const [rows] = await connection.sql.raw('SHOW INDEX FROM ??', [table]);
  const grouped = new Map<string, BootstrapMatrixPhysicalIndex>();
  for (const row of rows as Array<Record<string, any>>) {
    const name = String(row.Key_name);
    const current = grouped.get(name) ?? {
      name,
      columns: [],
      unique: Number(row.Non_unique) === 0,
    };
    current.columns[Number(row.Seq_in_index) - 1] = String(row.Column_name);
    grouped.set(name, current);
  }
  return [...grouped.values()];
}

function hasIndex(
  indexes: BootstrapMatrixPhysicalIndex[],
  columns: string[],
  unique?: boolean,
): boolean {
  return indexes.some((index) => {
    const hasExpectedPrefix = columns.every(
      (column, indexPosition) => index.columns[indexPosition] === column,
    );
    const remainingColumns = index.columns.slice(columns.length);
    const hasOnlyGeneratedTieBreaker =
      remainingColumns.length === 0 ||
      (remainingColumns.length === 1 &&
        (remainingColumns[0] === 'id' || remainingColumns[0] === '_id'));
    return (
      hasExpectedPrefix &&
      hasOnlyGeneratedTieBreaker &&
      (unique === undefined || index.unique === unique)
    );
  });
}

async function assertTargetState(
  connection: MatrixConnection,
  scenario: RandomizedBootstrapScenario,
): Promise<void> {
  const { assertions, target } = scenario;
  assert.equal(
    await tableExists(connection, assertions.sourceParentTable),
    false,
  );
  assert.equal(
    await tableExists(connection, assertions.targetParentTable),
    true,
  );
  assert.equal(await tableExists(connection, assertions.childTable), true);
  assert.equal(await tableExists(connection, assertions.droppedTable), false);
  assert.equal(await tableExists(connection, assertions.addedTable), true);

  assert.equal(
    await columnExists(
      connection,
      assertions.targetParentTable,
      assertions.renamedColumn.to,
    ),
    true,
  );
  assert.equal(
    await columnExists(
      connection,
      assertions.targetParentTable,
      assertions.renamedColumn.from,
    ),
    false,
  );
  assert.equal(
    await columnExists(
      connection,
      assertions.targetParentTable,
      assertions.addedColumn,
    ),
    true,
  );
  assert.equal(
    await findColumnMetadata(
      connection,
      assertions.targetParentTable,
      assertions.removedParentColumn,
    ),
    null,
  );
  assert.equal(
    await findColumnMetadata(
      connection,
      assertions.childTable,
      assertions.removedChildColumn,
    ),
    null,
  );

  const relations = await relationNames(connection, assertions.childTable);
  assert.ok(relations.includes(assertions.renamedRelation.to));
  assert.ok(relations.includes(assertions.addedRelation));
  assert.ok(!relations.includes(assertions.renamedRelation.from));
  assert.ok(!relations.includes(assertions.removedRelation));

  const metadata = await findTableMetadata(
    connection,
    assertions.targetParentTable,
  );
  const targetTable = target.snapshot[assertions.targetParentTable];
  assert.deepEqual(normalizeJson(metadata.indexes), targetTable.indexes);
  assert.deepEqual(normalizeJson(metadata.uniques), targetTable.uniques);

  const indexes = await physicalIndexes(
    connection,
    assertions.targetParentTable,
  );
  assert.ok(
    hasIndex(indexes, assertions.healing.indexColumns, false),
    `Expected non-unique index on ${assertions.healing.indexColumns.join(',')}; observed=${JSON.stringify(indexes)}`,
  );
  assert.ok(
    hasIndex(indexes, [assertions.renamedColumn.to], true),
    `Expected unique index on ${assertions.renamedColumn.to}; observed=${JSON.stringify(indexes)}`,
  );

  const parent = await findRecord(
    connection,
    assertions.targetParentTable,
    recordId(scenario.seed, 1),
  );
  const child = await findRecord(
    connection,
    assertions.childTable,
    recordId(scenario.seed, 2),
  );
  assert.ok(parent, 'Expected upgraded parent sentinel');
  assert.ok(child, 'Expected upgraded child sentinel');
  assert.equal(
    parent[assertions.renamedColumn.to],
    assertions.sentinel.parentLabel,
  );
  assert.equal(
    String(parent[assertions.modifiedColumn]),
    String(assertions.sentinel.parentCounter),
  );
  assert.equal(child.payload, assertions.sentinel.childPayload);
  await assertInitialized(connection);
}

async function injectHealingDrift(
  connection: MatrixConnection,
  scenario: RandomizedBootstrapScenario,
): Promise<void> {
  const { assertions } = scenario;
  const table = await findTableMetadata(connection, assertions.healing.table);
  const indexes = await physicalIndexes(connection, assertions.healing.table);
  const index = indexes.find((candidate) =>
    hasIndex([candidate], assertions.healing.indexColumns, false),
  );
  assert.ok(index, 'Expected target healing index before drift injection');

  if (connection.database === 'mongodb') {
    await connection.mongo
      .collection('enfyra_column')
      .updateOne(
        { table: table._id, name: assertions.healing.metadataColumn },
        { $set: { description: 'corrupted-by-bootstrap-matrix' } },
      );
    await connection.mongo
      .collection(assertions.healing.table)
      .updateMany({}, { $unset: { [assertions.healing.physicalColumn]: '' } });
    await connection.mongo
      .collection(assertions.healing.table)
      .dropIndex(index.name);
  } else {
    await connection
      .sql('enfyra_column')
      .where({ tableId: table.id, name: assertions.healing.metadataColumn })
      .update({ description: 'corrupted-by-bootstrap-matrix' });
    await connection.sql.schema.alterTable(
      assertions.healing.table,
      (builder) => {
        builder.dropColumn(assertions.healing.physicalColumn);
      },
    );
  }

  assert.equal(
    await columnExists(
      connection,
      assertions.healing.table,
      assertions.healing.physicalColumn,
    ),
    false,
  );
  assert.ok(
    !hasIndex(
      await physicalIndexes(connection, assertions.healing.table),
      assertions.healing.indexColumns,
      false,
    ),
  );
  await setBootstrapRequired(connection);
}

async function assertHealingState(
  connection: MatrixConnection,
  scenario: RandomizedBootstrapScenario,
): Promise<void> {
  const { assertions } = scenario;
  const column = await findColumnMetadata(
    connection,
    assertions.healing.table,
    assertions.healing.metadataColumn,
  );
  assert.equal(column?.description, assertions.healing.expectedDescription);
  assert.equal(
    await columnExists(
      connection,
      assertions.healing.table,
      assertions.healing.physicalColumn,
    ),
    true,
  );
  assert.ok(
    hasIndex(
      await physicalIndexes(connection, assertions.healing.table),
      assertions.healing.indexColumns,
      false,
    ),
    'Expected healing to restore the target index',
  );
  const parent = await findRecord(
    connection,
    assertions.targetParentTable,
    recordId(scenario.seed, 1),
  );
  assert.ok(parent);
  assert.equal(
    parent[assertions.renamedColumn.to],
    assertions.sentinel.parentLabel,
  );
}

async function bootRuntime(
  artifacts: RandomizedBootstrapScenario['source'],
  expectedBootstrapRuns: number,
): Promise<void> {
  const [
    { asValue },
    { buildContainer },
    { init, shutdown },
    bootstrap,
    cacheEvents,
  ] = await Promise.all([
    import('awilix'),
    import('../../src/container'),
    import('../../src/init'),
    import('../../src/engines/bootstrap/services/bootstrap-definition.service'),
    import('../../src/shared/utils/cache-events.constants'),
  ]);
  const container = buildContainer();
  container.register({
    bootstrapDefinitionService: asValue(
      new bootstrap.BootstrapDefinitionService(undefined, artifacts),
    ),
  });
  const initializer = container.cradle.firstRunInitializer;
  const originalRun = initializer.run.bind(initializer);
  let bootstrapRuns = 0;
  initializer.run = async () => {
    bootstrapRuns++;
    await originalRun();
  };
  let systemReady = 0;
  container.cradle.eventEmitter.on(
    cacheEvents.CACHE_EVENTS.SYSTEM_READY,
    () => {
      systemReady++;
    },
  );

  let primaryError: unknown;
  try {
    await init(container);
    assert.equal(systemReady, 1, 'Expected exactly one SYSTEM_READY event');
    assert.equal(
      bootstrapRuns,
      expectedBootstrapRuns,
      'Unexpected firstRunInitializer.run count',
    );
  } catch (error) {
    primaryError = error;
  }

  try {
    await shutdown(container);
  } catch (error) {
    if (primaryError) {
      throw new AggregateError(
        [primaryError, error],
        'Boot failed and shutdown also failed',
      );
    }
    throw error;
  }
  if (primaryError) throw primaryError;
}

async function runLifecycleCase(
  database: BootstrapMatrixDatabase,
  scenario: RandomizedBootstrapScenario,
): Promise<void> {
  const connection = await openMatrixConnection(database);
  try {
    console.log(
      `[bootstrap-matrix] ${database} seed=${scenario.seed} stage=install`,
    );
    await bootRuntime(scenario.source, 1);
    await insertSentinels(connection, scenario);
    await assertInitialized(connection);

    console.log(
      `[bootstrap-matrix] ${database} seed=${scenario.seed} stage=upgrade`,
    );
    await setBootstrapRequired(connection);
    await bootRuntime(scenario.target, 1);
    await assertTargetState(connection, scenario);

    console.log(
      `[bootstrap-matrix] ${database} seed=${scenario.seed} stage=healing`,
    );
    await injectHealingDrift(connection, scenario);
    await bootRuntime(scenario.target, 1);
    await assertTargetState(connection, scenario);
    await assertHealingState(connection, scenario);

    console.log(
      `[bootstrap-matrix] ${database} seed=${scenario.seed} stage=idempotent`,
    );
    await setBootstrapRequired(connection);
    await bootRuntime(scenario.target, 1);
    await assertTargetState(connection, scenario);

    console.log(
      `[bootstrap-matrix] ${database} seed=${scenario.seed} stage=stable-boot`,
    );
    await bootRuntime(scenario.target, 0);
    await assertTargetState(connection, scenario);
  } finally {
    await closeMatrixConnection(connection);
  }
}

async function runChildCase(): Promise<void> {
  const database = process.env.MATRIX_DATABASES as BootstrapMatrixDatabase;
  assert.ok(
    database === 'postgres' || database === 'mysql' || database === 'mongodb',
    'Matrix child requires exactly one supported database',
  );
  const seed = Number(required(CHILD_SEED));
  assert.ok(Number.isSafeInteger(seed), `${CHILD_SEED} must be a safe integer`);
  const { createRandomizedBootstrapScenario } =
    await import('./bootstrap-randomized-scenario');
  await runLifecycleCase(database, createRandomizedBootstrapScenario(seed));
}

const entrypoint =
  process.env.BOOTSTRAP_MATRIX_VALIDATE_ONLY === '1'
    ? async () => undefined
    : process.env[CHILD_MODE] === '1'
      ? runChildCase
      : runCoordinator;
entrypoint()
  .then(() => {
    if (process.env[CHILD_MODE] === '1') process.exit(0);
  })
  .catch((error) => {
    console.error(
      error instanceof Error ? error.stack || error.message : String(error),
    );
    process.exit(1);
  });
