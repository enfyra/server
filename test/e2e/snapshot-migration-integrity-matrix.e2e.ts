import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { spawn, type ChildProcess } from 'node:child_process';
import { knex, type Knex } from 'knex';
import { Long, MongoClient, ObjectId, type Db } from 'mongodb';
import { Redis } from 'ioredis';
import { PROVISION_LOCK_KEY } from '../../src/shared/utils/constant';
import { MetadataPhysicalMigrationHelper } from '../../src/engines/bootstrap/utils/metadata-physical-migration.util';
import { MetadataTableRenameService } from '../../src/engines/bootstrap/services/metadata-migration/metadata-table-rename.service';
import { applySqlSchemaMigrations } from '../../src/shared/utils/provision-schema-migration';
import {
  buildMongoValidationSchema,
  MONGO_VALIDATION_ACTION,
  MONGO_VALIDATION_LEVEL,
} from '../../src/engines/mongo/utils/mongo-validation-schema.util';
import { MONGO_COLUMN_TYPE_MIGRATIONS } from '../../src/shared/utils/column-type.util';
import { getEnfyraVersion } from '../../src/shared/utils/enfyra-version.util';

type SqlDatabase = 'postgres' | 'mysql';

/**
 * The oldest version the current declarations can upgrade from. A database that
 * records this version must converge onto the current target, and one that records
 * an older version must refuse to start.
 */
const LEGACY_SOURCE_VERSION = '2.2.19-patch-1';
const UNSUPPORTED_SOURCE_VERSION = '2.2.18';

type ServerOptions = {
  adminPassword: string;
  nodeName: string;
  secretKey: string;
};

type MongoLegacyFixture = {
  collectionName: string;
  tableId: ObjectId;
  rowId: ObjectId;
  freshSystemCollectionOptions: Record<string, any>;
};

const LEGACY_MONGO_TYPE_BY_NATIVE = new Map([
  ['string', 'varchar'],
  ['bool', 'boolean'],
  ['long', 'bigint'],
  ['double', 'float'],
  ['date', 'datetime'],
  ['json', 'simple-json'],
  ['objectId', 'ObjectId'],
]);

const LEGACY_MONGO_TYPE_OPTIONS = [
  'int',
  'varchar',
  'text',
  'longtext',
  'boolean',
  'uuid',
  'ObjectId',
  'bigint',
  'date',
  'datetime',
  'timestamp',
  'enum',
  'simple-json',
  'code',
  'array-select',
  'richtext',
  'float',
];

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function sqlConnection(database: SqlDatabase, databaseName?: string) {
  const prefix = database === 'postgres' ? 'POSTGRES' : 'MYSQL';
  return {
    host: process.env[`MATRIX_${prefix}_HOST`] || '127.0.0.1',
    port: Number(
      process.env[`MATRIX_${prefix}_PORT`] ||
        (database === 'postgres' ? 5432 : 3306),
    ),
    user: required(`MATRIX_${prefix}_USER`),
    password: required(`MATRIX_${prefix}_PASSWORD`),
    database: databaseName || required(`MATRIX_${prefix}_DATABASE`),
  };
}

function createKnex(database: SqlDatabase, databaseName?: string): Knex {
  return knex({
    client: database === 'postgres' ? 'pg' : 'mysql2',
    connection: sqlConnection(database, databaseName),
  });
}

function buildDatabaseUri(
  protocol: 'postgresql' | 'mysql' | 'mongodb',
  connection: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
  },
  authSource?: string,
): string {
  const url = new URL(`${protocol}://${connection.host}`);
  url.port = String(connection.port);
  url.username = connection.user;
  url.password = connection.password;
  url.pathname = `/${connection.database}`;
  if (authSource) url.searchParams.set('authSource', authSource);
  return url.toString();
}

function redisUri(): string {
  const base = process.env.MATRIX_REDIS_URI || 'redis://127.0.0.1:6379';
  const url = new URL(base);
  url.pathname = `/${process.env.MATRIX_REDIS_DB || '13'}`;
  return url.toString();
}

function killServerProcess(child: ChildProcess, signal: NodeJS.Signals): void {
  try {
    if (process.platform !== 'win32' && child.pid) {
      process.kill(-child.pid, signal);
      return;
    }
  } catch {}
  child.kill(signal);
}

async function stopServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      if (child.exitCode === null) killServerProcess(child, 'SIGKILL');
    }, 10_000);
    child.once('exit', () => {
      clearTimeout(timer);
      resolve();
    });
    killServerProcess(child, 'SIGTERM');
  });
}

async function clearProvisionLock(nodeName: string): Promise<void> {
  const redis = new Redis(redisUri(), { maxRetriesPerRequest: 1 });
  try {
    await redis.del(`${nodeName}:${PROVISION_LOCK_KEY}`, PROVISION_LOCK_KEY);
  } finally {
    await redis.quit();
  }
}

async function bootServer(
  databaseUri: string,
  port: number,
  options: ServerOptions,
): Promise<ChildProcess> {
  const child = spawn('yarn', ['tsx', 'src/main.ts'], {
    cwd: process.cwd(),
    detached: process.platform !== 'win32',
    env: {
      ...process.env,
      ADMIN_EMAIL: 'snapshot-migration-integrity@localhost.test',
      ADMIN_PASSWORD: options.adminPassword,
      BOOTSTRAP_VERBOSE: process.env.MATRIX_BOOTSTRAP_VERBOSE || '0',
      DB_URI: databaseUri,
      MONGO_FORCE_APP_TRANSACTION: '0',
      NODE_ENV: 'test',
      NODE_NAME: options.nodeName,
      PORT: String(port),
      REDIS_URI: redisUri(),
      SECRET_KEY: options.secretKey,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';

  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      killServerProcess(child, 'SIGKILL');
      reject(
        new Error(
          `Server boot timed out on port ${port}: ${output.slice(-4000)}`,
        ),
      );
    }, 90_000);
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      if (!output.includes(`HTTP listening on port ${port}`)) return;
      clearTimeout(timer);
      resolve();
    };
    child.stdout?.on('data', onData);
    child.stderr?.on('data', onData);
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(
        new Error(
          `Server exited before listening with code ${code}: ${output.slice(-4000)}`,
        ),
      );
    });
  });

  return child;
}

/**
 * Boots the server expecting the bootstrap to reject the recorded version, and
 * returns the captured output so the caller can assert on the refusal reason.
 */
async function bootServerExpectingFailure(
  databaseUri: string,
  port: number,
  options: ServerOptions,
): Promise<string> {
  const child = spawn('yarn', ['tsx', 'src/main.ts'], {
    cwd: process.cwd(),
    detached: process.platform !== 'win32',
    env: {
      ...process.env,
      ADMIN_EMAIL: 'snapshot-migration-integrity@localhost.test',
      ADMIN_PASSWORD: options.adminPassword,
      BOOTSTRAP_VERBOSE: '0',
      DB_URI: databaseUri,
      MONGO_FORCE_APP_TRANSACTION: '0',
      NODE_ENV: 'test',
      NODE_NAME: options.nodeName,
      PORT: String(port),
      REDIS_URI: redisUri(),
      SECRET_KEY: options.secretKey,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';

  try {
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        killServerProcess(child, 'SIGKILL');
        reject(
          new Error(
            `Server neither listened nor exited within 90s on port ${port}: ${output.slice(-4000)}`,
          ),
        );
      }, 90_000);
      const onData = (chunk: Buffer) => {
        output += chunk.toString();
        if (output.includes(`HTTP listening on port ${port}`)) {
          clearTimeout(timer);
          killServerProcess(child, 'SIGKILL');
          reject(
            new Error(
              `Server accepted an unsupported recorded version and started listening: ${output.slice(-4000)}`,
            ),
          );
        }
      };
      child.stdout?.on('data', onData);
      child.stderr?.on('data', onData);
      child.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  } finally {
    await stopServer(child);
  }

  return output;
}

async function assertSqlDatetimeOverlapPreserved(
  database: SqlDatabase,
  target: Knex,
): Promise<void> {
  const legacyTable = 'legacy_datetime_records';
  const canonicalTable = 'canonical_datetime_records';
  const expected = '2026-09-20 12:34:56';

  await target.schema.createTable(legacyTable, (table) => {
    table.integer('id').primary();
    table.string('name').notNullable();
    table.datetime('occurredAt').notNullable();
  });
  await target.schema.createTable(canonicalTable, (table) => {
    table.integer('id').primary();
    table.string('name').notNullable();
  });
  await target(legacyTable).insert({
    id: 1,
    name: 'legacy',
    occurredAt: target.raw(
      database === 'postgres'
        ? `TIMESTAMP '${expected}'`
        : `CAST('${expected}' AS DATETIME)`,
    ),
  });

  const queryBuilderService = {
    isMongoDb: () => false,
    getKnex: () => target,
  } as any;
  const physicalMigration = new MetadataPhysicalMigrationHelper({
    queryBuilderService,
  });
  const service = new MetadataTableRenameService({
    queryBuilderService,
    physicalMigration,
    systemCoreTableResolver: {
      getTableName: async () => 'enfyra_table',
    } as any,
    verbose: () => undefined,
  });

  await service.runTableRenames(
    [
      {
        from: legacyTable,
        to: canonicalTable,
        mergeKeys: ['name'],
      },
    ],
    false,
  );

  const info = await target(canonicalTable).columnInfo('occurredAt');
  assert.match(
    String(info.type).toLowerCase(),
    /date|timestamp/,
    `${database} overlap migration changed DATETIME to ${info.type}`,
  );
  const formatted =
    database === 'postgres'
      ? await target.raw(
          `SELECT to_char("occurredAt", 'YYYY-MM-DD HH24:MI:SS') AS value FROM ?? WHERE name = ?`,
          [canonicalTable, 'legacy'],
        )
      : await target.raw(
          `SELECT DATE_FORMAT(??, '%Y-%m-%d %H:%i:%s') AS value FROM ?? WHERE name = ?`,
          ['occurredAt', canonicalTable, 'legacy'],
        );
  const value =
    database === 'postgres'
      ? formatted.rows[0]?.value
      : formatted[0]?.[0]?.value;
  assert.equal(
    value,
    expected,
    `${database} overlap migration changed the datetime value`,
  );
}

async function assertSqlForeignKeyColumnRemoval(target: Knex): Promise<void> {
  const parentTable = 'integrity_fk_parents';
  const childTable = 'integrity_fk_children';
  await target.schema.createTable(parentTable, (table) => {
    table.integer('id').primary();
  });
  await target.schema.createTable(childTable, (table) => {
    table.integer('id').primary();
    table.integer('parentId').references('id').inTable(parentTable);
  });
  await target(parentTable).insert({ id: 1 });
  await target(childTable).insert({ id: 1, parentId: 1 });

  await applySqlSchemaMigrations(target, {
    tables: [
      {
        _unique: { name: { _eq: childTable } },
        columnsToRemove: ['parentId'],
      },
    ],
  });

  assert.equal(await target.schema.hasColumn(childTable, 'parentId'), false);
}

async function forceLegacySqlCodeStorage(
  database: SqlDatabase,
  target: Knex,
): Promise<void> {
  for (const columnName of ['sourceCode', 'compiledCode']) {
    if (database === 'postgres') {
      await target.raw(
        'ALTER TABLE ?? ALTER COLUMN ?? TYPE VARCHAR USING ??::varchar',
        ['enfyra_route_handler', columnName, columnName],
      );
    } else {
      await target.raw('ALTER TABLE ?? MODIFY COLUMN ?? TEXT NULL', [
        'enfyra_route_handler',
        columnName,
      ]);
    }
  }
}

async function downgradeRecordedVersion(
  target: Knex,
  version: string,
): Promise<void> {
  await target('enfyra_setting').update({
    enfyraVersion: version,
    isInit: true,
  });
}

async function getSqlCodeStorageType(
  database: SqlDatabase,
  target: Knex,
  columnName: string,
): Promise<string> {
  if (database === 'postgres') {
    const result = await target.raw(
      `SELECT data_type
       FROM information_schema.columns
       WHERE table_schema = current_schema()
         AND table_name = ?
         AND column_name = ?`,
      ['enfyra_route_handler', columnName],
    );
    return String(result.rows?.[0]?.data_type ?? '').toLowerCase();
  }
  const result = await target.raw(
    `SELECT DATA_TYPE
     FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME = ?
       AND COLUMN_NAME = ?`,
    ['enfyra_route_handler', columnName],
  );
  return String(result[0]?.[0]?.DATA_TYPE ?? '').toLowerCase();
}

async function runSqlLegacyScriptUpgrade(
  database: SqlDatabase,
  port: number,
): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_integrity_${suffix}`;
  const admin = createKnex(database);
  let target: Knex | null = null;
  let server: ChildProcess | null = null;
  const nodeName = `snapshot-migration-integrity-${database}-${suffix}`;
  const options = {
    adminPassword: `e2e-${randomUUID()}`,
    nodeName,
    secretKey: `snapshot-migration-integrity-${randomUUID()}`,
  };

  try {
    await admin.raw('CREATE DATABASE ??', [databaseName]);
    target = createKnex(database, databaseName);
    const databaseUri = buildDatabaseUri(
      database === 'postgres' ? 'postgresql' : 'mysql',
      sqlConnection(database, databaseName),
    );
    await clearProvisionLock(nodeName);
    server = await bootServer(databaseUri, port, options);
    await stopServer(server);
    server = null;

    await assertSqlDatetimeOverlapPreserved(database, target);
    await assertSqlForeignKeyColumnRemoval(target);

    const table = await target('enfyra_table')
      .where({ name: 'enfyra_route_handler' })
      .first();
    const row = await target('enfyra_route_handler')
      .orderBy('id', 'asc')
      .first();
    assert.ok(table?.id);
    assert.ok(row?.id);

    const legacyScript = 'return { migrated: true };';
    await target('enfyra_route_handler')
      .where({ id: row.id })
      .update({ sourceCode: legacyScript, compiledCode: legacyScript });
    await forceLegacySqlCodeStorage(database, target);
    assert.equal(
      await getSqlCodeStorageType(database, target, 'sourceCode'),
      database === 'postgres' ? 'character varying' : 'text',
    );
    const installedVersion = await target('enfyra_setting')
      .first()
      .then((setting) => setting?.enfyraVersion);
    assert.equal(
      installedVersion,
      getEnfyraVersion(),
      `${database} bootstrap did not record the running Enfyra version`,
    );
    await downgradeRecordedVersion(target, LEGACY_SOURCE_VERSION);

    server = await bootServer(databaseUri, port, options);
    const upgraded = await target('enfyra_route_handler')
      .where({ id: row.id })
      .first();
    assert.equal(
      upgraded.sourceCode,
      legacyScript,
      `${database} upgrade lost the stored route-handler source`,
    );
    assert.equal(
      await getSqlCodeStorageType(database, target, 'sourceCode'),
      database === 'postgres' ? 'text' : 'longtext',
      `${database} did not converge sourceCode to its native large-text target`,
    );
    assert.equal(
      await getSqlCodeStorageType(database, target, 'compiledCode'),
      database === 'postgres' ? 'text' : 'longtext',
      `${database} did not converge compiledCode to its native large-text target`,
    );
    const oversizedCode = `return ${JSON.stringify('x'.repeat(70 * 1024))};`;
    await target('enfyra_route_handler')
      .where({ id: row.id })
      .update({ sourceCode: oversizedCode, compiledCode: oversizedCode });
    const oversizedRoundTrip = await target('enfyra_route_handler')
      .where({ id: row.id })
      .first();
    assert.equal(oversizedRoundTrip.sourceCode, oversizedCode);
    assert.equal(oversizedRoundTrip.compiledCode, oversizedCode);
    const upgradedSetting = await target('enfyra_setting').first();
    assert.equal(
      upgradedSetting.isInit === true || upgradedSetting.isInit === 1,
      true,
    );
    assert.equal(
      upgradedSetting.enfyraVersion,
      getEnfyraVersion(),
      `${database} upgrade did not advance the recorded Enfyra version`,
    );

    await stopServer(server);
    server = null;
    await downgradeRecordedVersion(target, UNSUPPORTED_SOURCE_VERSION);
    const refusal = await bootServerExpectingFailure(
      databaseUri,
      port,
      options,
    );
    assert.match(
      refusal,
      /oldest supported upgrade source/,
      `${database} did not refuse a version older than the oldest supported upgrade source`,
    );
    assert.equal(
      (await target('enfyra_setting').first()).enfyraVersion,
      UNSUPPORTED_SOURCE_VERSION,
      `${database} advanced the recorded version despite refusing the upgrade`,
    );
  } finally {
    if (server) await stopServer(server);
    await target?.destroy();
    if (database === 'postgres') {
      await admin.raw('DROP DATABASE IF EXISTS ?? WITH (FORCE)', [
        databaseName,
      ]);
    } else {
      await admin.raw('DROP DATABASE IF EXISTS ??', [databaseName]);
    }
    await admin.destroy();
  }
}

async function getMongoColumnMetadata(
  db: Db,
  tableName: string,
  columnName: string,
): Promise<any> {
  const table = await db
    .collection('enfyra_table')
    .findOne({ name: tableName });
  assert.ok(table?._id, `Mongo metadata table ${tableName} is missing`);
  const column = await db.collection('enfyra_column').findOne({
    table: table._id,
    name: columnName,
  });
  assert.ok(
    column?._id,
    `Mongo metadata column ${tableName}.${columnName} is missing`,
  );
  return column;
}

async function assertMongoFreshNativeTarget(db: Db): Promise<void> {
  const legacyTypes = MONGO_COLUMN_TYPE_MIGRATIONS.map(({ from }) => from);
  assert.equal(
    await db.collection('enfyra_column').countDocuments({
      type: { $in: legacyTypes },
    }),
    0,
    'fresh Mongo install persisted legacy SQL column aliases',
  );
  const types = new Set(
    await db.collection('enfyra_column').distinct<string>('type'),
  );
  for (const type of [
    'string',
    'bool',
    'long',
    'date',
    'json',
    'objectId',
    'code',
    'array-select',
  ]) {
    assert.equal(
      types.has(type),
      true,
      `fresh Mongo install is missing ${type}`,
    );
  }
  const typeColumn = await getMongoColumnMetadata(db, 'enfyra_column', 'type');
  assert.equal(Array.isArray(typeColumn.options), true);
  assert.equal(typeColumn.options.includes('object'), true);
  assert.equal(typeColumn.options.includes('json'), true);
  assert.equal(typeColumn.options.includes('simple-json'), false);
  assert.equal(typeColumn.options.includes('varchar'), false);

  const tables = await db.collection('enfyra_table').find({}).toArray();
  const tableById = new Map(
    tables.map((table) => [String(table._id), table.name]),
  );
  const jsonColumns = await db
    .collection('enfyra_column')
    .find({ type: { $in: ['object', 'array'] } })
    .toArray();
  const mismatches: string[] = [];
  for (const column of jsonColumns) {
    const tableName = tableById.get(String(column.table));
    if (!tableName || !column.name) continue;
    const rows = await db
      .collection(tableName)
      .find(
        { [column.name]: { $ne: null } },
        { projection: { [column.name]: 1 } },
      )
      .toArray();
    for (const row of rows) {
      const value = row[column.name];
      const actual = Array.isArray(value)
        ? 'array'
        : value !== null && typeof value === 'object'
          ? 'object'
          : typeof value;
      if (actual !== column.type) {
        mismatches.push(
          `${tableName}.${column.name}:${column.type}<-${actual}`,
        );
      }
    }
  }
  assert.deepEqual(
    [...new Set(mismatches)].sort(),
    [],
    'fresh Mongo native JSON metadata disagrees with persisted BSON values',
  );
}

async function createLegacyMongoFixture(
  db: Db,
  suffix: string,
): Promise<MongoLegacyFixture> {
  const collectionName = `legacy_native_types_${suffix}`;
  let step = 'load metadata templates';
  try {
    const routeHandlerTable = await db
      .collection('enfyra_table')
      .findOne({ name: 'enfyra_route_handler' });
    assert.ok(routeHandlerTable?._id);
    const primaryTemplate = await db.collection('enfyra_column').findOne({
      table: routeHandlerTable._id,
      isPrimary: true,
    });
    const scalarTemplate = await db.collection('enfyra_column').findOne({
      table: routeHandlerTable._id,
      name: 'sourceCode',
    });
    assert.ok(primaryTemplate?._id);
    assert.ok(scalarTemplate?._id);

    const tableId = new ObjectId();
    const rowId = new ObjectId();
    const now = new Date();
    step = 'insert legacy table metadata';
    await db.collection('enfyra_table').insertOne({
      ...routeHandlerTable,
      _id: tableId,
      name: collectionName,
      alias: 'Legacy native type fixture',
      description: 'Disposable Mongo native type migration fixture',
      isSystem: false,
      uniques: null,
      indexes: null,
      createdAt: now,
      updatedAt: now,
    });

    const legacyDefinitions = [
      {
        template: primaryTemplate,
        name: '_id',
        type: 'ObjectId',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isUpdatable: false,
      },
      { name: 'legacyString', type: 'varchar', isNullable: false },
      { name: 'legacyBoolean', type: 'boolean', isNullable: false },
      { name: 'legacyLong', type: 'bigint', isNullable: false },
      { name: 'legacyDouble', type: 'float', isNullable: false },
      { name: 'legacyDate', type: 'datetime', isNullable: false },
      { name: 'legacyObject', type: 'simple-json', isNullable: false },
      { name: 'semanticCode', type: 'code', isNullable: false },
      { name: 'nativeArray', type: 'array', isNullable: false },
    ];
    const columns = legacyDefinitions.map((definition) => ({
      ...(definition.template ?? scalarTemplate),
      _id: new ObjectId(),
      name: definition.name,
      type: definition.type,
      table: tableId,
      isPrimary: definition.isPrimary ?? false,
      isGenerated: definition.isGenerated ?? false,
      isNullable: definition.isNullable,
      isSystem: false,
      isUpdatable: definition.isUpdatable ?? true,
      isPublished: true,
      isEncrypted: false,
      defaultValue: null,
      options: null,
      description: null,
      placeholder: null,
      createdAt: now,
      updatedAt: now,
    }));
    step = 'insert legacy column metadata';
    await db.collection('enfyra_column').insertMany(columns);
    step = 'create legacy physical collection';
    await db.createCollection(collectionName, {
      validator: { $jsonSchema: buildMongoValidationSchema(columns) },
      validationLevel: 'strict',
      validationAction: 'error',
    });
    const oversizedCode = `return ${JSON.stringify('m'.repeat(70 * 1024))};`;
    step = 'insert legacy physical document';
    await db.collection(collectionName).insertOne({
      _id: rowId,
      legacyString: 'legacy',
      legacyBoolean: true,
      legacyLong: Long.fromString('9223372036854775807'),
      legacyDouble: 1.5,
      legacyDate: now,
      legacyObject: { legacy: true },
      semanticCode: oversizedCode,
      nativeArray: ['legacy'],
      createdAt: now,
      updatedAt: now,
    });

    const systemCollection = 'enfyra_oauth_config';
    const systemInfo = await db
      .listCollections({ name: systemCollection })
      .next();
    assert.ok(systemInfo?.options?.validator);
    const freshSystemCollectionOptions = systemInfo.options;

    step = 'downgrade Mongo metadata types';
    for (const [nativeType, legacyType] of LEGACY_MONGO_TYPE_BY_NATIVE) {
      await db
        .collection('enfyra_column')
        .updateMany(
          { type: nativeType },
          { $set: { type: legacyType, updatedAt: now } },
        );
    }
    const typeColumn = await getMongoColumnMetadata(
      db,
      'enfyra_column',
      'type',
    );
    await db
      .collection('enfyra_column')
      .updateOne(
        { _id: typeColumn._id },
        { $set: { options: LEGACY_MONGO_TYPE_OPTIONS, updatedAt: now } },
      );
    step = 'drift Mongo collection validator';
    await db.command({
      collMod: systemCollection,
      validator: {},
      validationLevel: 'off',
      validationAction: 'warn',
    });

    const legacyTypes = MONGO_COLUMN_TYPE_MIGRATIONS.map(({ from }) => from);
    assert.equal(
      (await db.collection('enfyra_column').countDocuments({
        type: { $in: legacyTypes },
      })) > 0,
      true,
      'Mongo fixture did not enter a legacy metadata state',
    );
    const legacyInfo = await db
      .listCollections({ name: systemCollection })
      .next();
    assert.equal(legacyInfo?.options?.validationLevel, 'off');
    assert.equal(legacyInfo?.options?.validationAction, 'warn');

    return {
      collectionName,
      tableId,
      rowId,
      freshSystemCollectionOptions,
    };
  } catch (error) {
    const details =
      error && typeof error === 'object' && 'errInfo' in error
        ? ` ${JSON.stringify((error as any).errInfo)}`
        : '';
    throw new Error(
      `Mongo legacy fixture failed during ${step}: ${error instanceof Error ? error.message : String(error)}${details}`,
      { cause: error },
    );
  }
}

async function assertMongoNativeUpgradeTarget(
  db: Db,
  fixture: MongoLegacyFixture,
): Promise<void> {
  const legacyTypes = MONGO_COLUMN_TYPE_MIGRATIONS.map(({ from }) => from);
  assert.equal(
    await db.collection('enfyra_column').countDocuments({
      type: { $in: legacyTypes },
    }),
    0,
    'Mongo upgrade left legacy SQL column aliases in metadata',
  );

  const expectedTypes = new Map([
    ['_id', 'objectId'],
    ['legacyString', 'string'],
    ['legacyBoolean', 'bool'],
    ['legacyLong', 'long'],
    ['legacyDouble', 'double'],
    ['legacyDate', 'date'],
    ['legacyObject', 'json'],
    ['semanticCode', 'code'],
    ['nativeArray', 'array'],
  ]);
  const upgradedColumns = await db
    .collection('enfyra_column')
    .find({ table: fixture.tableId })
    .toArray();
  assert.equal(upgradedColumns.length, expectedTypes.size);
  for (const column of upgradedColumns) {
    assert.equal(
      column.type,
      expectedTypes.get(column.name),
      `Mongo upgrade produced the wrong native type for ${column.name}`,
    );
  }

  const typeColumn = await getMongoColumnMetadata(db, 'enfyra_column', 'type');
  for (const type of [
    'string',
    'long',
    'double',
    'bool',
    'object',
    'objectId',
    'json',
  ]) {
    assert.equal(typeColumn.options.includes(type), true);
  }
  for (const type of [
    'varchar',
    'boolean',
    'bigint',
    'float',
    'simple-json',
    'ObjectId',
  ]) {
    assert.equal(typeColumn.options.includes(type), false);
  }

  const systemInfo = await db
    .listCollections({ name: 'enfyra_oauth_config' })
    .next();
  assert.deepEqual(
    systemInfo?.options?.validator,
    fixture.freshSystemCollectionOptions.validator,
  );
  assert.equal(systemInfo?.options?.validationLevel, MONGO_VALIDATION_LEVEL);
  assert.equal(systemInfo?.options?.validationAction, MONGO_VALIDATION_ACTION);

  const oversizedCode = `return ${JSON.stringify('n'.repeat(70 * 1024))};`;
  const insertedId = new ObjectId();
  const now = new Date();
  await db.collection(fixture.collectionName).insertOne({
    _id: insertedId,
    legacyString: 'native',
    legacyBoolean: false,
    legacyLong: Long.fromString('9223372036854775806'),
    legacyDouble: 2.5,
    legacyDate: now,
    legacyObject: { native: true },
    semanticCode: oversizedCode,
    nativeArray: ['native'],
    createdAt: now,
    updatedAt: now,
  });
  const roundTrip = await db
    .collection(fixture.collectionName)
    .findOne({ _id: insertedId });
  assert.ok(roundTrip?._id instanceof ObjectId);
  assert.equal(Long.isLong(roundTrip?.legacyLong), true);
  assert.equal(roundTrip?.legacyLong.toString(), '9223372036854775806');
  assert.equal(roundTrip?.legacyBoolean, false);
  assert.equal(roundTrip?.legacyDouble, 2.5);
  assert.ok(roundTrip?.legacyDate instanceof Date);
  assert.deepEqual(roundTrip?.legacyObject, { native: true });
  assert.deepEqual(roundTrip?.nativeArray, ['native']);
  assert.equal(roundTrip?.semanticCode, oversizedCode);

  const legacyRoundTrip = await db
    .collection(fixture.collectionName)
    .findOne({ _id: fixture.rowId });
  assert.equal(Long.isLong(legacyRoundTrip?.legacyLong), true);
  assert.equal(legacyRoundTrip?.legacyLong.toString(), '9223372036854775807');
  assert.equal(legacyRoundTrip?.semanticCode.length > 64 * 1024, true);
}

async function runMongoUpgrade(port: number): Promise<void> {
  const suffix = randomUUID().replaceAll('-', '').slice(0, 12);
  const databaseName = `enfyra_snapshot_integrity_${suffix}`;
  const user = required('MATRIX_MONGO_USER');
  const password = required('MATRIX_MONGO_PASSWORD');
  const host = process.env.MATRIX_MONGO_HOST || '127.0.0.1';
  const mongoPort = Number(process.env.MATRIX_MONGO_PORT || 27017);
  const authSource = process.env.MATRIX_MONGO_AUTH_DATABASE || 'admin';
  const databaseUri = buildDatabaseUri(
    'mongodb',
    { host, port: mongoPort, user, password, database: databaseName },
    authSource,
  );
  const client = new MongoClient(databaseUri);
  let server: ChildProcess | null = null;
  const nodeName = `snapshot-migration-integrity-mongodb-${suffix}`;
  const options = {
    adminPassword: `e2e-${randomUUID()}`,
    nodeName,
    secretKey: `snapshot-migration-integrity-${randomUUID()}`,
  };

  try {
    await client.connect();
    const db = client.db(databaseName);
    await clearProvisionLock(nodeName);
    server = await bootServer(databaseUri, port, options);
    await stopServer(server);
    server = null;

    await assertMongoFreshNativeTarget(db);
    const nativeTypeFixture = await createLegacyMongoFixture(db, suffix);

    const oauthConfigTable = await db
      .collection('enfyra_table')
      .findOne({ name: 'enfyra_oauth_config' });
    const routeHandler = await db
      .collection('enfyra_route_handler')
      .findOne({});
    const sourceCodeColumn = await db.collection('enfyra_column').findOne({
      table: oauthConfigTable?._id,
      name: 'sourceCode',
    });
    assert.ok(oauthConfigTable?._id);
    assert.ok(routeHandler?._id);
    assert.ok(sourceCodeColumn?._id);

    const legacyScript = 'return { migrated: true };';
    await db
      .collection('enfyra_route_handler')
      .updateOne(
        { _id: routeHandler._id },
        { $set: { sourceCode: legacyScript, compiledCode: legacyScript } },
      );
    const installedSetting = await db.collection('enfyra_setting').findOne({});
    assert.equal(
      installedSetting?.enfyraVersion,
      getEnfyraVersion(),
      'mongodb bootstrap did not record the running Enfyra version',
    );
    await db
      .collection('enfyra_setting')
      .updateOne(
        {},
        { $set: { enfyraVersion: LEGACY_SOURCE_VERSION, isInit: true } },
      );

    server = await bootServer(databaseUri, port, options);
    const upgradedHandler = await db
      .collection('enfyra_route_handler')
      .findOne({ _id: routeHandler._id });
    const upgradedSourceCodeColumn = await db
      .collection('enfyra_column')
      .findOne({
        table: oauthConfigTable._id,
        name: 'sourceCode',
      });
    assert.equal(
      upgradedHandler?.sourceCode,
      legacyScript,
      'mongodb upgrade lost the stored route-handler source',
    );
    assert.equal(
      String(upgradedSourceCodeColumn?._id),
      String(sourceCodeColumn._id),
      'mongodb same-name column modification replaced the metadata identity',
    );
    await assertMongoNativeUpgradeTarget(db, nativeTypeFixture);
    const upgradedSetting = await db.collection('enfyra_setting').findOne({});
    assert.equal(upgradedSetting?.isInit, true);
    assert.equal(
      upgradedSetting?.enfyraVersion,
      getEnfyraVersion(),
      'mongodb upgrade did not advance the recorded Enfyra version',
    );

    await stopServer(server);
    server = null;
    await db
      .collection('enfyra_setting')
      .updateOne(
        {},
        { $set: { enfyraVersion: UNSUPPORTED_SOURCE_VERSION, isInit: true } },
      );
    const refusal = await bootServerExpectingFailure(
      databaseUri,
      port,
      options,
    );
    assert.match(
      refusal,
      /oldest supported upgrade source/,
      'mongodb did not refuse a version older than the oldest supported upgrade source',
    );
    assert.equal(
      (await db.collection('enfyra_setting').findOne({}))?.enfyraVersion,
      UNSUPPORTED_SOURCE_VERSION,
      'mongodb advanced the recorded version despite refusing the upgrade',
    );
  } finally {
    if (server) await stopServer(server);
    await client.db(databaseName).dropDatabase();
    await client.close();
  }
}

async function main(): Promise<void> {
  const selected = new Set(
    (process.env.MATRIX_DATABASES || 'postgres,mysql,mongodb')
      .split(',')
      .map((value) => value.trim())
      .filter(Boolean),
  );
  const supported = new Set(['postgres', 'mysql', 'mongodb']);
  const unknown = [...selected].filter((value) => !supported.has(value));
  if (unknown.length > 0) {
    throw new Error(
      `Unsupported MATRIX_DATABASES value: ${unknown.join(', ')}`,
    );
  }

  if (selected.has('postgres')) {
    await runSqlLegacyScriptUpgrade('postgres', 18141);
    console.log('PostgreSQL snapshot migration integrity E2E passed');
  }
  if (selected.has('mysql')) {
    await runSqlLegacyScriptUpgrade('mysql', 18142);
    console.log('MySQL snapshot migration integrity E2E passed');
  }
  if (selected.has('mongodb')) {
    await runMongoUpgrade(18143);
    console.log('MongoDB snapshot migration integrity E2E passed');
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
