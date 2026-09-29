import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { MongoClient } from 'mongodb';
import { knex, type Knex } from 'knex';
import { asValue } from 'awilix';
import { buildContainer } from '../../src/container';
import { bootstrapSourceArtifacts } from '../../src/data';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap';
import type { BootstrapSourceArtifacts } from '../../src/engines/bootstrap/types';
import {
  normalizeMongoJunctionId,
  replaceMongoJunctionRows,
  resolveMongoJunctionMetadata,
} from '../../src/domain/bootstrap/utils/mongo-junction-writer.util';
import { getSqlJunctionMetadata } from '../../src/domain/bootstrap/utils/sql-junction-metadata.util';
import { replaceSqlJunctionRows } from '../../src/domain/bootstrap/utils/sql-junction-writer.util';
import { init, initBootstrap, shutdown } from '../../src/init';
import { getCurrentDatabaseSchema } from '../../src/engines/knex/utils/provision/schema-comparison';
import { getShortSqlIdentifier } from '../../src/engines/knex/utils/sql-physical-schema-contract';

type Database = 'postgres' | 'mysql' | 'mongodb';
type SqlDatabase = Exclude<Database, 'mongodb'>;
type Stage = 'fresh' | 'runtime' | 'legacy' | 'upgrade' | 'oldest-legacy' | 'oldest-upgrade';

const CHILD_MODE = 'ROUTE_METHOD_CONFIG_MATRIX_CHILD';
const CHILD_STAGE = 'ROUTE_METHOD_CONFIG_MATRIX_STAGE';
const SUPPORTED_DATABASES: Database[] = ['postgres', 'mysql', 'mongodb'];
const HANDLER_MARKER = 'route-method-config-upgrade-e2e';

function sqlConnection(database: SqlDatabase, databaseName?: string) {
  const prefix = database === 'postgres' ? 'POSTGRES' : 'MYSQL';
  return {
    host: process.env[`MATRIX_${prefix}_HOST`] || '127.0.0.1',
    port: Number(
      process.env[`MATRIX_${prefix}_PORT`] ||
        (database === 'postgres' ? 5432 : 3306),
    ),
    user: process.env[`MATRIX_${prefix}_USER`] || 'root',
    password: process.env[`MATRIX_${prefix}_PASSWORD`] || '1234',
    database:
      databaseName ||
      process.env[`MATRIX_${prefix}_DATABASE`] ||
      (database === 'postgres' ? 'enfyra' : 'enfyra_matrix'),
  };
}

function sqlClient(database: SqlDatabase, databaseName?: string): Knex {
  return knex({
    client: database === 'postgres' ? 'pg' : 'mysql2',
    connection: sqlConnection(database, databaseName),
  });
}

function databaseUri(database: SqlDatabase, databaseName: string): string {
  const connection = sqlConnection(database, databaseName);
  const protocol = database === 'postgres' ? 'postgresql' : 'mysql';
  const uri = new URL(`${protocol}://${connection.host}`);
  uri.port = String(connection.port);
  uri.username = connection.user;
  uri.password = connection.password;
  uri.pathname = `/${databaseName}`;
  return uri.toString();
}

function mongoSettings() {
  return {
    host: process.env.MATRIX_MONGO_HOST || '127.0.0.1',
    port: Number(process.env.MATRIX_MONGO_PORT || 27017),
    user: process.env.MATRIX_MONGO_USER || 'enfyra_admin',
    password: process.env.MATRIX_MONGO_PASSWORD || 'enfyra_password_123',
    authDatabase: process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin',
  };
}

function mongoUri(databaseName: string): string {
  const settings = mongoSettings();
  const uri = new URL(`mongodb://${settings.host}`);
  uri.port = String(settings.port);
  uri.username = settings.user;
  uri.password = settings.password;
  uri.pathname = `/${databaseName}`;
  uri.searchParams.set('authSource', settings.authDatabase);
  return uri.toString();
}

function selectedDatabases(): Database[] {
  const requested = (
    process.env.MATRIX_DATABASES || SUPPORTED_DATABASES.join(',')
  )
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean) as Database[];
  const unsupported = requested.filter(
    (database) => !SUPPORTED_DATABASES.includes(database),
  );
  if (unsupported.length > 0) {
    throw new Error(
      `Unsupported MATRIX_DATABASES value: ${unsupported.join(', ')}`,
    );
  }
  return [...new Set(requested)];
}

async function createDatabase(database: Database, name: string): Promise<void> {
  if (database === 'mongodb') return;
  const admin = sqlClient(database);
  try {
    await admin.raw('CREATE DATABASE ??', [name]);
  } finally {
    await admin.destroy();
  }
}

async function dropDatabase(database: Database, name: string): Promise<void> {
  if (database === 'mongodb') {
    const client = new MongoClient(mongoUri(name));
    try {
      await client.connect();
      await client.db(name).dropDatabase();
    } finally {
      await client.close();
    }
    return;
  }

  const admin = sqlClient(database);
  try {
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [name]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [name]);
    }
  } finally {
    await admin.destroy();
  }
}

function buildLegacyArtifacts(oldest = false): BootstrapSourceArtifacts {
  const sources = structuredClone(bootstrapSourceArtifacts);
  const snapshot = sources.snapshot;
  delete snapshot.enfyra_route_method_config;
  delete snapshot.enfyra_route_method_config_file_field;
  sources.defaultData.enfyra_route = (sources.defaultData.enfyra_route as any[]).filter(
    (route) => !['/enfyra_route_method_config', '/enfyra_route_method_config_file_field'].includes(route.path),
  );
  for (const migration of sources.dataMigrations) {
    if (!migration.data.enfyra_route) continue;
    migration.data.enfyra_route = (migration.data.enfyra_route as any[]).filter(
      (route) => !['/enfyra_route_method_config', '/enfyra_route_method_config_file_field'].includes(route._unique?.path?._eq),
    );
  }

  stripRelation(snapshot, 'enfyra_route', 'methodConfigs');
  stripRelation(snapshot, 'enfyra_method', 'methodConfigs');
  stripRelation(snapshot, 'enfyra_route_handler', 'routeMethodConfig');
  stripRelation(snapshot, 'enfyra_route_permission', 'routeMethodConfigs');
  stripRelation(snapshot, 'enfyra_pre_hook', 'routeMethodConfigs');
  stripRelation(snapshot, 'enfyra_post_hook', 'routeMethodConfigs');
  stripRelation(snapshot, 'enfyra_guard', 'routeMethodConfigs');
  stripColumn(snapshot, 'enfyra_guard', 'appliesToAllRouteMethods');
  snapshot.enfyra_route_handler.uniques = (
    snapshot.enfyra_route_handler.uniques || []
  ).filter(
    (unique: string[]) =>
      !unique.some((field) => field === 'routeMethodConfig'),
  );
  sources.migrations = sources.migrations.filter(
    (migration) => migration.toVersion !== '2.3.1',
  );
  sources.dataMigrations = sources.dataMigrations.filter(
    (migration) => migration.toVersion !== '2.3.1',
  );
  if (oldest) {
    sources.migrations = [];
    sources.dataMigrations = [];
    snapshot.enfyra_setting.columns.push({
      name: 'uniquesIndexesRepaired',
      sqlType: { type: 'boolean' },
      mongoType: { type: 'bool' },
      isNullable: false,
      isSystem: true,
      defaultValue: false,
    });
    const typeColumn = snapshot.enfyra_column.columns.find(
      (column: any) => column.name === 'type',
    );
    typeColumn.sqlType = { type: 'varchar' };
    typeColumn.mongoType = { type: 'string' };
    delete typeColumn.options;
  }
  return sources;
}

function stripRelation(
  snapshot: Record<string, any>,
  table: string,
  propertyName: string,
): void {
  snapshot[table].relations = (snapshot[table].relations || []).filter(
    (relation: any) => relation.propertyName !== propertyName,
  );
}

function stripColumn(
  snapshot: Record<string, any>,
  table: string,
  name: string,
): void {
  snapshot[table].columns = (snapshot[table].columns || []).filter(
    (column: any) => column.name !== name,
  );
}

async function runChildStage(stage: Stage): Promise<void> {
  const container = buildContainer();
  if (stage === 'legacy' || stage === 'oldest-legacy') {
    const legacyDefinition = new BootstrapDefinitionService(
      { databaseConfigService: container.cradle.databaseConfigService },
      buildLegacyArtifacts(stage === 'oldest-legacy'),
    );
    container.register({
      bootstrapDefinitionService: asValue(legacyDefinition),
      routeMethodConfigBackfillService: asValue({ run: async () => undefined }),
    });
  }

  let primaryError: unknown;
  try {
    if (stage === 'legacy' || stage === 'oldest-legacy') {
      await initBootstrap(container);
      await prepareLegacyUpgradeState(
        container.cradle.queryBuilderService,
        stage === 'oldest-legacy' ? '2.2.19-patch-1' : '2.3.0',
      );
    } else {
      await init(container);
      await assertCompleteMatrix(container.cradle.queryBuilderService, stage === 'runtime' ? 'fresh' : stage);
      if (stage === 'runtime') {
        await assertRuntimeMutations(container);
      }
      if (!container.cradle.queryBuilderService.isMongoDb()) {
        await assertPhysicalSqlNames(container.cradle.queryBuilderService.getKnex());
      }
      if (stage === 'oldest-upgrade') {
        await assertOldestUpgradeSchema(container.cradle.queryBuilderService);
      }
    }
    process.stdout.write(`[route-method-config-e2e] stage=${stage} assertions=passed\n`);
  } catch (error) {
    primaryError = error;
  }

  try {
    await shutdown(container);
  } catch (error) {
    if (primaryError) {
      throw new AggregateError(
        [primaryError, error],
        `${stage} failed and shutdown also failed`,
      );
    }
    throw error;
  }
  if (primaryError) throw primaryError;
}

async function assertRuntimeMutations(container: ReturnType<typeof buildContainer>): Promise<void> {
  const { queryBuilderService, dynamicRepositoryFactory, routeCacheService } = container.cradle;
  const idField = queryBuilderService.getPkField();
  const context = { $query: {}, $user: { isRootAdmin: true, roles: [] } } as any;
  const routeRepo = dynamicRepositoryFactory.create('enfyra_route', context);
  const methodRepo = dynamicRepositoryFactory.create('enfyra_method', context);
  const configRepo = dynamicRepositoryFactory.create('enfyra_route_method_config', context);
  const path = `/route-config-matrix-${randomUUID()}`;
  const routeResult = await routeRepo.create({ data: { path, isEnabled: true } });
  const routeId = routeResult.data?.[0]?.[idField];
  assert.ok(routeId != null, 'Route mutation did not return an id');

  const methodsBefore = await queryBuilderService.find({
    table: 'enfyra_method', fields: [idField], limit: 0,
  });
  let configs = await queryBuilderService.find({
    table: 'enfyra_route_method_config',
    filter: { route: { [idField]: { _eq: routeId } } },
    fields: [idField, 'method.id', 'available', 'timeout'], limit: 0,
  });
  assert.equal(configs.data.length, methodsBefore.data.length, 'Route creation did not complete its method matrix');
  assert.ok(configs.data.every((cell: any) => cell.available === false && cell.timeout === 30_000));
  assert.equal((await routeCacheService.getRoutes()).find((route: any) => route.path === path)?.methodConfigs.length, methodsBefore.data.length);

  const methodResult = await methodRepo.create({ data: { name: `MATRIX${randomUUID().replaceAll('-', '').slice(0, 8)}` } });
  const methodId = methodResult.data?.[0]?.[idField];
  assert.ok(methodId != null, 'Method mutation did not return an id');
  const routes = await queryBuilderService.find({ table: 'enfyra_route', fields: [idField], limit: 0 });
  const methodsAfter = await queryBuilderService.find({ table: 'enfyra_method', fields: [idField, 'name'], limit: 0 });
  assert.equal(methodsAfter.data.length, methodsBefore.data.length + 1, 'Method creation was not persisted');
  const newMethodConfigs = await queryBuilderService.find({
    table: 'enfyra_route_method_config',
    filter: { method: { [idField]: { _eq: methodId } } },
    fields: [idField, 'route.id', 'available', 'timeout'], limit: 0,
  });
  assert.equal(newMethodConfigs.data.length, routes.data.length, 'Method creation did not complete the route matrix');
  assert.ok(newMethodConfigs.data.every((cell: any) => cell.available === false && cell.timeout === 30_000));
  const cell = newMethodConfigs.data.find((candidate: any) => String(relationId(candidate.route)) === String(routeId));
  assert.ok(cell, 'New route/method cell is missing');
  assert.equal((await routeCacheService.getRoutes()).find((route: any) => route.path === path)?.methodConfigs.length, methodsBefore.data.length + 1);

  await configRepo.update({ id: cell[idField], data: { available: true, isPublic: true, timeout: 12_345 } });
  configs = await queryBuilderService.find({
    table: 'enfyra_route_method_config',
    filter: { [idField]: { _eq: cell[idField] } },
    fields: ['available', 'isPublic', 'timeout'], limit: 1,
  });
  assert.deepEqual([configs.data[0]?.available, configs.data[0]?.isPublic, configs.data[0]?.timeout], [true, true, 12_345]);
  const cached = (await routeCacheService.getRoutes()).find((route: any) => route.path === path);
  const cachedCell = cached?.methodConfigs.find((candidate: any) => String(relationId(candidate.method)) === String(methodId));
  assert.deepEqual([cachedCell?.available, cachedCell?.isPublic, cachedCell?.timeout], [true, true, 12_345]);
}

async function assertOldestUpgradeSchema(queryBuilderService: any): Promise<void> {
  const idField = queryBuilderService.getPkField();
  const typeColumn = await queryBuilderService.find({
    table: 'enfyra_column',
    filter: {
      _and: [
        { name: { _eq: 'type' } },
        { table: { name: { _eq: 'enfyra_column' } } },
      ],
    },
    fields: ['type', 'options'],
    limit: 1,
  });
  assert.equal(typeColumn.data?.[0]?.type, 'enum', 'First migration step did not update column type metadata');
  const settings = await queryBuilderService.find({
    table: 'enfyra_setting',
    fields: ['enfyraVersion', 'isInit'],
    limit: 1,
  });
  assert.equal(settings.data?.[0]?.enfyraVersion, '2.3.1');
  assert.equal(settings.data?.[0]?.isInit, true);
  if (queryBuilderService.isMongoDb()) {
    const row = await queryBuilderService.getMongoDb().collection('enfyra_setting').findOne({});
    assert.equal(Object.prototype.hasOwnProperty.call(row ?? {}, 'uniquesIndexesRepaired'), false);
  } else {
    assert.equal(await queryBuilderService.getKnex().schema.hasColumn('enfyra_setting', 'uniquesIndexesRepaired'), false);
  }
  const configs = await queryBuilderService.find({
    table: 'enfyra_route_method_config',
    fields: [idField],
    limit: 1,
  });
  assert.ok(configs.data?.[0], 'Second migration step did not create route method configs');
}

async function assertPhysicalSqlNames(connection: Knex): Promise<void> {
  const tableName = 'enfyra_route_method_config_file_field';
  const schema = await getCurrentDatabaseSchema(connection, tableName);
  const expectedUnique = getShortSqlIdentifier(
    'uq', tableName, 'routeMethodConfigId', 'name',
  );
  const expectedIndex = getShortSqlIdentifier(
    'idx', tableName, 'routeMethodConfigId', 'sort',
  );
  assert.ok(schema.uniques.some((item) => item.name === expectedUnique), 'File-field unique uses a different physical name');
  assert.ok(schema.indexes.some((item) => item.name === expectedIndex), 'File-field sort index uses a different physical name');
}

async function assertCompleteMatrix(
  queryBuilderService: any,
  stage: 'fresh' | 'upgrade' | 'oldest-upgrade',
): Promise<void> {
  const idField = queryBuilderService.getPkField();
  const [routes, methods, configs] = await Promise.all([
    queryBuilderService.find({
      table: 'enfyra_route',
      fields: [idField],
      limit: 0,
    }),
    queryBuilderService.find({
      table: 'enfyra_method',
      fields: [idField],
      limit: 0,
    }),
    queryBuilderService.find({
      table: 'enfyra_route_method_config',
      fields: [
        idField,
        'route.id',
        'method.id',
        'available',
        'isPublic',
        'skipRoleGuard',
        'timeout',
        'requestBodyType',
        'handler.id',
      ],
      limit: 0,
    }),
  ]);

  const expected = routes.data.length * methods.data.length;
  assert.equal(configs.data.length, expected, `${stage} matrix size mismatch`);
  const keys = new Set(
    configs.data.map(
      (config: any) =>
        `${String(relationId(config.route))}:${String(relationId(config.method))}`,
    ),
  );
  assert.equal(keys.size, expected, `${stage} matrix contains duplicate cells`);
  for (const config of configs.data) {
    assert.equal(typeof config.available, 'boolean');
    assert.equal(typeof config.isPublic, 'boolean');
    assert.equal(typeof config.skipRoleGuard, 'boolean');
    assert.ok(Number.isInteger(config.timeout) && config.timeout > 0);
    assert.equal(config.requestBodyType, 'none');
  }

  if (stage === 'upgrade' || stage === 'oldest-upgrade') {
    const handlers = await queryBuilderService.find({
      table: 'enfyra_route_handler',
      filter: { description: { _eq: HANDLER_MARKER } },
      fields: [
        idField,
        'route.id',
        'method.id',
        'timeout',
        'routeMethodConfig.id',
      ],
      limit: 1,
    });
    const handler = handlers.data?.[0];
    assert.ok(handler, 'Legacy handler marker was not preserved');
    const configId = relationId(handler.routeMethodConfig);
    assert.ok(configId, 'Legacy handler was not linked to a canonical config');
    const config = configs.data.find(
      (candidate: any) => String(candidate[idField]) === String(configId),
    );
    assert.ok(config, 'Linked route method config is missing');
    assert.equal(config.timeout, 60_000, 'Legacy zero timeout was not migrated');
    assert.equal(config.available, true, 'Legacy available flag was not migrated');
    assert.equal(config.isPublic, true, 'Legacy public flag was not migrated');
    assert.equal(
      config.skipRoleGuard,
      true,
      'Legacy skip-role flag was not migrated',
    );
  }
}

async function prepareLegacyUpgradeState(
  queryBuilderService: any,
  sourceVersion: '2.3.0' | '2.2.19-patch-1',
): Promise<void> {
  const idField = queryBuilderService.getPkField();
  const handlers = await queryBuilderService.find({
    table: 'enfyra_route_handler',
    fields: [idField, 'route.id', 'method.id'],
    limit: 1,
  });
  const handler = handlers.data?.[0];
  assert.ok(handler, 'Legacy fixture has no route handler');
  const handlerId = handler[idField];
  const routeId = relationId(handler.route);
  const methodId = relationId(handler.method);
  assert.ok(handlerId != null && routeId != null && methodId != null);

  await queryBuilderService.update('enfyra_route_handler', handlerId, {
    timeout: 0,
    description: HANDLER_MARKER,
  });
  for (const propertyName of [
    'availableMethods',
    'publicMethods',
    'skipRoleGuardMethods',
  ]) {
    const metadata = queryBuilderService.isMongoDb()
      ? await resolveMongoJunctionMetadata(queryBuilderService, {
          sourceTable: 'enfyra_route',
          propertyName,
          targetTable: 'enfyra_method',
        })
      : await getSqlJunctionMetadata(queryBuilderService, {
          sourceTable: 'enfyra_route',
          propertyName,
          targetTable: 'enfyra_method',
        });
    const input = {
      ...metadata,
      sourceId: queryBuilderService.isMongoDb()
        ? normalizeMongoJunctionId(routeId)
        : routeId,
      targetIds: [
        queryBuilderService.isMongoDb()
          ? normalizeMongoJunctionId(methodId)
          : methodId,
      ],
    };
    if (queryBuilderService.isMongoDb()) {
      await replaceMongoJunctionRows(queryBuilderService, input);
    } else {
      await replaceSqlJunctionRows(queryBuilderService, input);
    }
  }

  const settings = await queryBuilderService.find({
    table: 'enfyra_setting',
    fields: [idField],
    limit: 1,
  });
  const setting = settings.data?.[0];
  assert.ok(setting?.[idField] != null, 'Legacy fixture has no setting row');
  await queryBuilderService.update('enfyra_setting', setting[idField], {
    isInit: true,
    enfyraVersion: sourceVersion,
  });
}

function relationId(value: any): unknown {
  if (value == null) return null;
  if (typeof value === 'object') return value._id ?? value.id ?? null;
  return value;
}

async function runStage(
  database: Database,
  databaseName: string,
  stage: Stage,
  nodeName: string,
): Promise<string> {
  const script = process.argv[1];
  const dbUri =
    database === 'mongodb'
      ? mongoUri(databaseName)
      : databaseUri(database, databaseName);
  const child = spawn('yarn', ['tsx', script], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      [CHILD_MODE]: '1',
      [CHILD_STAGE]: stage,
      DB_URI: dbUri,
      REDIS_URI: process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379/13',
      NODE_NAME: nodeName,
      NODE_ENV: 'test',
      SECRET_KEY: `route-method-config-${randomUUID()}`,
      ADMIN_EMAIL: `route-method-config-${database}@localhost.test`,
      ADMIN_PASSWORD: `Matrix-${randomUUID()}-A1!`,
      STARTUP_VERBOSE: '1',
      BOOTSTRAP_VERBOSE: '1',
      LOG_LEVEL: 'info',
      LOG_DISABLE_CONSOLE: '0',
      MONGO_FORCE_APP_TRANSACTION: '0',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout?.on('data', (chunk) => {
    output += chunk.toString();
  });
  child.stderr?.on('data', (chunk) => {
    output += chunk.toString();
  });

  const exitCode = await new Promise<number | null>((resolve, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`${database} ${stage} timed out`));
    }, 180_000);
    child.once('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.once('exit', (code) => {
      clearTimeout(timeout);
      resolve(code);
    });
  });
  if (exitCode !== 0) {
    throw new Error(
      `${database} ${stage} failed with code ${exitCode}:\n${output.slice(-12_000)}`,
    );
  }
  assert.match(
    output,
    new RegExp(`stage=${stage} assertions=passed`),
    `${database} ${stage} did not report assertions`,
  );
  return output;
}

function assertVerboseTrace(output: string, database: Database, stage: Stage): void {
  assert.ok(
    output.includes('Route method config matrix:'),
    `${database} ${stage} omitted matrix verbose trace`,
  );
  assert.ok(
    output.includes('cache-query') &&
      output.includes('cache=RouteCache') &&
      output.includes('table=enfyra_route'),
    `${database} ${stage} omitted route cache query trace`,
  );
  assert.ok(
    output.includes('cache-reload phase=transformed') &&
      output.includes('id=route'),
    `${database} ${stage} omitted route cache transform trace`,
  );
  assert.ok(
    output.includes('runtime-cache phase=activated id=route'),
    `${database} ${stage} omitted route runtime activation trace`,
  );
}

async function runDatabase(database: Database): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const freshDatabase = `enfyra_route_config_fresh_${suffix}`;
  const upgradeDatabase = `enfyra_route_config_upgrade_${suffix}`;
  const freshNodeName = `route-config-fresh-${database}-${suffix}`;
  const upgradeNodeName = `route-config-upgrade-${database}-${suffix}`;

  try {
    await createDatabase(database, freshDatabase);
    const freshOutput = await runStage(
      database,
      freshDatabase,
      'fresh',
      freshNodeName,
    );
    assertVerboseTrace(freshOutput, database, 'fresh');
    const runtimeOutput = await runStage(database, freshDatabase, 'runtime', freshNodeName);
    assert.match(runtimeOutput, /stage=runtime assertions=passed/);
  } finally {
    await dropDatabase(database, freshDatabase).catch(() => undefined);
  }

  try {
    await createDatabase(database, upgradeDatabase);
    await runStage(database, upgradeDatabase, 'legacy', upgradeNodeName);
    const upgradeOutput = await runStage(
      database,
      upgradeDatabase,
      'upgrade',
      upgradeNodeName,
    );
    assertVerboseTrace(upgradeOutput, database, 'upgrade');
  } finally {
    await dropDatabase(database, upgradeDatabase).catch(() => undefined);
  }

  const oldestDatabase = `enfyra_route_config_oldest_${suffix}`;
  try {
    await createDatabase(database, oldestDatabase);
    await runStage(database, oldestDatabase, 'oldest-legacy', upgradeNodeName + '-oldest');
    const oldestOutput = await runStage(database, oldestDatabase, 'oldest-upgrade', upgradeNodeName + '-oldest');
    assertVerboseTrace(oldestOutput, database, 'oldest-upgrade');
    assert.match(oldestOutput, /Version-scoped bootstrap declarations resolved from 2\.2\.19-patch-1/);
    const firstStepCommands = [...oldestOutput.matchAll(/Migration command 2\.3\.0:/g)];
    const secondStepCommands = [...oldestOutput.matchAll(/Migration command 2\.3\.1:/g)];
    assert.ok(firstStepCommands.length > 0, `${database} skipped the 2.3.0 schema step`);
    assert.ok(secondStepCommands.length > 0, `${database} skipped the 2.3.1 schema step`);
    assert.ok(
      firstStepCommands.at(-1)!.index! < secondStepCommands[0].index!,
      `${database} interleaved schema commands across versions`,
    );
  } finally {
    await dropDatabase(database, oldestDatabase).catch(() => undefined);
  }

  process.stdout.write(`[route-method-config-e2e] database=${database} passed\n`);
}

async function main(): Promise<void> {
  if (process.env[CHILD_MODE] === '1') {
    const stage = process.env[CHILD_STAGE] as Stage;
    assert.ok(['fresh', 'runtime', 'legacy', 'upgrade', 'oldest-legacy', 'oldest-upgrade'].includes(stage));
    await runChildStage(stage);
    return;
  }

  for (const database of selectedDatabases()) {
    await runDatabase(database);
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
