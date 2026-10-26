import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import knex, { type Knex } from 'knex';
import { Double, Int32, Long, MongoClient, ObjectId } from 'mongodb';
import { toMongoTargetColumnType } from '../../../src/shared/utils/column-type.util';
import { MONGO_COLUMN_TYPE_OPTIONS } from '../../../src/shared/types/column-type.types';
import { buildMongoFullIndexSpecs } from '../../../src/engines/mongo/utils/mongo-physical-schema-contract';
import { buildMongoValidationSchema } from '../../../src/engines/mongo/utils/mongo-validation-schema.util';
import type {
  CloneCatalog,
  CloneConversionState,
  ClonePhysicalColumn,
} from '../types/production-clone-conversion.types';

const directory = process.env.CROSS_DB_PRIVATE_DIRECTORY!;
const state: CloneConversionState = JSON.parse(
  readFileSync(join(directory, 'state.json'), 'utf8'),
);
const catalog: CloneCatalog = JSON.parse(
  readFileSync(join(directory, 'source-catalog.json'), 'utf8'),
);
const backend = process.env.CONVERT_BACKEND;
assert.ok(backend === 'mysql' || backend === 'mongodb');
assert.match(state.sourceDatabase, /^enfyra_1endpoint_source_[a-f0-9]{32}$/);
const database =
  backend === 'mysql' ? state.mysqlDatabase : state.mongoDatabase;
assert.match(database, /^enfyra_1endpoint_clone_[a-f0-9]{32}$/);

function containerEnv(): Record<string, string> {
  return {
    POSTGRES_USER: process.env.CLONE_PG_USER!,
    POSTGRES_PASSWORD: process.env.CLONE_PG_PASSWORD!,
    MYSQL_ROOT_PASSWORD: process.env.CLONE_MYSQL_PASSWORD!,
    MONGO_INITDB_ROOT_USERNAME: process.env.CLONE_MONGO_USER!,
    MONGO_INITDB_ROOT_PASSWORD: process.env.CLONE_MONGO_PASSWORD!,
  };
}
const pgEnv = containerEnv();
export const source = knex({
  client: 'pg',
  connection: {
    host: '127.0.0.1',
    port: 5432,
    user: pgEnv.POSTGRES_USER,
    password: pgEnv.POSTGRES_PASSWORD,
    database: state.sourceDatabase,
    options: '-c default_transaction_read_only=on',
  },
  pool: { min: 0, max: 1 },
});
const tablesById = new Map(
  catalog.tables.map((table) => [String(table.id), table.name as string]),
);
const columnsByTable = new Map<string, ClonePhysicalColumn[]>();
for (const column of catalog.columns) {
  const columns = columnsByTable.get(column.table_name) ?? [];
  columns.push(column);
  columnsByTable.set(column.table_name, columns);
}
const metadataByTable = new Map(
  [...columnsByTable.keys()].map((name) => [
    name,
    catalog.metadataColumns.filter(
      (column) => tablesById.get(String(column.tableId)) === name,
    ),
  ]),
);
const relationsByTable = new Map(
  [...columnsByTable.keys()].map((name) => [
    name,
    catalog.relations.filter(
      (relation) => tablesById.get(String(relation.sourceTableId)) === name,
    ),
  ]),
);
const objectId = (table: string, id: unknown) =>
  new ObjectId(
    createHash('sha256')
      .update(JSON.stringify([table, String(id)]))
      .digest('hex')
      .slice(0, 24),
  );
const parseJson = (value: any) => {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
};

export async function* batches(table: string) {
  const columns = columnsByTable.get(table)!;
  const primary = catalog.constraints.find(
    (constraint) =>
      constraint.table_name === table && constraint.contype === 'p',
  );
  const key = primary?.columns.length === 1 ? primary.columns[0] : undefined;
  let cursor: unknown;
  let offset = 0;
  while (true) {
    const query = source(table)
      .select(
        columns.map((column) =>
          column.data_type.includes('timestamp')
            ? source.raw('??::text as ??', [
                column.column_name,
                column.column_name,
              ])
            : column.column_name,
        ),
      )
      .limit(500);
    if (key) {
      query.orderBy(key);
      if (cursor !== undefined) query.where(key, '>', cursor);
    } else query.offset(offset);
    const rows = await query;
    if (rows.length === 0) return;
    yield rows;
    cursor = key ? rows.at(-1)![key] : undefined;
    offset += rows.length;
  }
}

function mysqlType(column: ClonePhysicalColumn): string {
  const quotedEnum = catalog.enums.find(
    (entry) => entry.typname === column.udt_name,
  );
  if (quotedEnum)
    return `ENUM(${quotedEnum.labels.map((value) => `'${value.replaceAll('\\', '\\\\').replaceAll("'", "''")}'`).join(',')})`;
  switch (column.data_type) {
    case 'integer':
      return 'INT';
    case 'bigint':
      return 'BIGINT';
    case 'boolean':
      return 'BOOLEAN';
    case 'real':
      return 'FLOAT';
    case 'double precision':
      return 'DOUBLE';
    case 'text':
      return 'LONGTEXT';
    case 'uuid':
      return 'CHAR(36)';
    case 'character varying':
      return `VARCHAR(${column.character_maximum_length ?? 255})`;
    case 'timestamp with time zone':
      return 'DATETIME(6)';
    default:
      throw new Error(
        `Unsupported source type ${column.data_type} in ${column.table_name}.${column.column_name}`,
      );
  }
}
function mysqlDefault(db: Knex, column: ClonePhysicalColumn): string {
  const value = column.column_default;
  if (
    value === null ||
    value.startsWith('nextval(') ||
    value === 'gen_random_uuid()' ||
    column.data_type === 'text'
  )
    return '';
  if (/^(CURRENT_TIMESTAMP|now\(\))$/i.test(value))
    return ' DEFAULT CURRENT_TIMESTAMP(6)';
  if (value === 'true' || value === 'false')
    return ` DEFAULT ${value === 'true' ? '1' : '0'}`;
  const literal = value.match(/^'((?:[^']|'')*)'(?:::.*)?$/);
  if (literal)
    return (
      ' DEFAULT ' + db.raw('?', [literal[1].replaceAll("''", "'")]).toString()
    );
  if (/^-?\d+(\.\d+)?$/.test(value)) return ` DEFAULT ${value}`;
  throw new Error(
    `Unsupported source default in ${column.table_name}.${column.column_name}`,
  );
}

async function convertMySql() {
  const values = containerEnv();
  const db = knex({
    client: 'mysql2',
    connection: {
      host: '127.0.0.1',
      port: 3306,
      user: 'root',
      password: values.MYSQL_ROOT_PASSWORD,
      timezone: 'Z',
      supportBigNumbers: true,
      bigNumberStrings: true,
      multipleStatements: false,
    },
    pool: { min: 0, max: 1 },
  });
  const counts: Record<string, number> =
    process.env.CONVERT_RESUME_CONSTRAINTS === '1'
      ? JSON.parse(readFileSync(join(directory, 'mysql-import.json'), 'utf8'))
          .counts
      : {};
  try {
    const resume = process.env.CONVERT_RESUME_CONSTRAINTS === '1';
    if (!resume) {
      assert.equal(
        (
          await db.raw(
            'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?',
            [database],
          )
        )[0].length,
        0,
      );
      await db.raw(
        'CREATE DATABASE ?? CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin',
        [database],
      );
    }
    await db.raw('USE ??', [database]);
    await db.raw("SET time_zone='+00:00'");
    if (!resume)
      for (const [table, columns] of columnsByTable) {
        const definitions = columns.map((column) => {
          const auto = column.column_default?.startsWith('nextval(');
          return (
            db.raw('??', [column.column_name]).toString() +
            ' ' +
            mysqlType(column) +
            (column.is_nullable === 'NO' ? ' NOT NULL' : ' NULL') +
            (auto ? ' AUTO_INCREMENT' : mysqlDefault(db, column))
          );
        });
        const primary = catalog.constraints.find(
          (constraint) =>
            constraint.table_name === table && constraint.contype === 'p',
        );
        if (primary)
          definitions.push(
            'PRIMARY KEY (' +
              primary.columns
                .map((column) => db.raw('??', [column]).toString())
                .join(',') +
              ')',
          );
        await db.raw(
          'CREATE TABLE ?? (' + definitions.join(',') + ') ENGINE=InnoDB',
          [table],
        );
        counts[table] = 0;
        for await (const rows of batches(table)) {
          for (const row of rows) {
            for (const column of columns)
              if (
                column.data_type.includes('timestamp') &&
                row[column.column_name] !== null
              ) {
                const value = String(row[column.column_name]);
                assert.match(value, /\+00$/);
                row[column.column_name] = value.slice(0, -3);
              }
          }
          await db(table).insert(rows);
          counts[table] += rows.length;
        }
        console.log(`CONVERT_PROGRESS ${backend} ${table} ${counts[table]}`);
      }
    if (!resume)
      for (const index of catalog.indexes) {
        const primary = catalog.constraints.find(
          (constraint) =>
            constraint.table_name === index.tablename &&
            constraint.contype === 'p' &&
            constraint.conname === index.indexname,
        );
        if (primary) continue;
        const match = index.indexdef.match(/USING btree \((.*)\)$/);
        assert.ok(match, 'Only plain btree source indexes may be translated');
        const columns = match[1]
          .split(',')
          .map((value) =>
            value.trim().replace(/^"|"$/g, '').replaceAll('""', '"'),
          );
        const unique = index.indexdef.startsWith('CREATE UNIQUE');
        await db.raw(
          `CREATE ${unique ? 'UNIQUE ' : ''}INDEX ?? ON ?? (${columns.map(() => '??').join(',')})`,
          [index.indexname, index.tablename, ...columns],
        );
      }
    const allocated = new Set<string>();
    const existingKeys = new Set(
      (
        await db.raw(
          'SELECT CONSTRAINT_NAME FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=?',
          [database],
        )
      )[0].map((row: any) => String(row.CONSTRAINT_NAME).toLowerCase()),
    );
    for (const constraint of catalog.constraints.filter(
      (entry) => entry.contype === 'f',
    )) {
      const originalName = constraint.conname;
      const name = allocated.has(originalName.toLowerCase())
        ? originalName.slice(0, 45) +
          '_' +
          createHash('sha256')
            .update(JSON.stringify([constraint.table_name, originalName]))
            .digest('hex')
            .slice(0, 12)
        : originalName;
      allocated.add(name.toLowerCase());
      if (existingKeys.has(name.toLowerCase())) continue;
      const action: Record<string, string> = {
        a: 'NO ACTION',
        r: 'RESTRICT',
        c: 'CASCADE',
        n: 'SET NULL',
        d: 'SET DEFAULT',
      };
      assert.ok(
        action[constraint.confdeltype] && action[constraint.confupdtype],
      );
      await db.raw(
        `ALTER TABLE ?? ADD CONSTRAINT ?? FOREIGN KEY (${constraint.columns.map(() => '??').join(',')}) REFERENCES ?? (${constraint.target_columns.map(() => '??').join(',')}) ON UPDATE ${action[constraint.confupdtype]} ON DELETE ${action[constraint.confdeltype]}`,
        [
          constraint.table_name,
          name,
          ...constraint.columns,
          constraint.target_table,
          ...constraint.target_columns,
        ],
      );
    }
    writeFileSync(
      join(directory, 'mysql-import.json'),
      JSON.stringify(
        {
          database,
          counts,
          originalPhysicalNamesRetained: true,
          constraints: catalog.constraints.filter(
            (entry) => entry.contype === 'f',
          ).length,
        },
        null,
        2,
      ),
      { mode: 0o600 },
    );
  } finally {
    await db.destroy();
  }
}

export function mongoRow(
  table: string,
  row: Record<string, any>,
): Record<string, any> {
  const result: Record<string, any> = {};
  const physical = columnsByTable.get(table)!;
  const metadata = metadataByTable.get(table)!;
  const primary = catalog.constraints.find(
    (constraint) =>
      constraint.table_name === table && constraint.contype === 'p',
  );
  const identity =
    primary?.columns.length === 1
      ? row[primary.columns[0]]
      : JSON.stringify(primary?.columns.map((key) => row[key]) ?? row);
  result._id = objectId(table, identity);
  const references = catalog.constraints.filter(
    (constraint) =>
      constraint.table_name === table && constraint.contype === 'f',
  );
  for (const column of physical) {
    const name = column.column_name;
    if (name === 'id') continue;
    const value = row[name];
    const relation = relationsByTable
      .get(table)!
      .find(
        (entry) =>
          entry.foreignKeyColumn === name &&
          !entry.mappedById &&
          ['many-to-one', 'one-to-one'].includes(entry.type),
      );
    const reference = references.find(
      (entry) => entry.columns.length === 1 && entry.columns[0] === name,
    );
    const field = relation?.propertyName ?? name;
    if (value === null) {
      result[field] = null;
      continue;
    }
    if (reference) {
      result[field] = objectId(reference.target_table!, value);
      continue;
    }
    const logical = metadata.find((entry) => entry.name === name);
    if (logical?.type === 'simple-json' || logical?.type === 'array-select')
      result[field] = parseJson(value);
    else if (column.data_type.includes('timestamp'))
      result[field] = new Date(value);
    else if (column.data_type === 'bigint')
      result[field] = Long.fromString(String(value));
    else if (column.data_type === 'integer')
      result[field] = new Int32(Number(value));
    else if (['real', 'double precision'].includes(column.data_type))
      result[field] = new Double(Number(value));
    else result[field] = value;
  }
  if (table === 'enfyra_column') {
    result.type = row.isPrimary
      ? 'objectId'
      : toMongoTargetColumnType(row.type);
    if (row.isPrimary) result.name = '_id';
    if (
      tablesById.get(String(row.tableId)) === 'enfyra_column' &&
      row.name === 'type'
    )
      result.options = MONGO_COLUMN_TYPE_OPTIONS;
  }
  if (table === 'enfyra_relation') {
    result.mappedBy =
      row.mappedById == null
        ? null
        : objectId('enfyra_relation', row.mappedById);
    delete result.mappedById;
    if (
      row.type === 'many-to-one' ||
      (row.type === 'one-to-one' && !row.mappedById)
    )
      result.foreignKeyColumn = row.propertyName;
    else if (row.mappedById)
      result.foreignKeyColumn =
        catalog.relations.find(
          (entry) => String(entry.id) === String(row.mappedById),
        )?.propertyName ?? null;
    if (result.referencedColumn === 'id') result.referencedColumn = '_id';
  }
  if (table === 'enfyra_table') {
    for (const field of ['uniques', 'indexes'])
      if (Array.isArray(result[field]))
        result[field] = result[field].map((group: string[]) =>
          group.map((name) => (name === 'id' ? '_id' : name)),
        );
  }
  return result;
}

async function convertMongo() {
  const values = containerEnv();
  const uri = new URL('mongodb://127.0.0.1:27017/admin');
  uri.username = values.MONGO_INITDB_ROOT_USERNAME;
  uri.password = values.MONGO_INITDB_ROOT_PASSWORD;
  const client = new MongoClient(uri.toString(), {
    serverSelectionTimeoutMS: 10000,
  });
  const counts: Record<string, number> = {};
  try {
    await client.connect();
    assert.ok(
      !(await client.db('admin').admin().listDatabases()).databases.some(
        (entry) => entry.name === database,
      ),
    );
    const db = client.db(database);
    for (const table of columnsByTable.keys()) {
      await db.createCollection(table);
      counts[table] = 0;
      for await (const rows of batches(table)) {
        const documents = rows.map((row) => mongoRow(table, row));
        await db.collection(table).insertMany(documents, { ordered: true });
        counts[table] += rows.length;
      }
      console.log(`CONVERT_PROGRESS ${backend} ${table} ${counts[table]}`);
    }
    for (const table of catalog.tables) {
      const columns = metadataByTable.get(table.name)!.map((column) => ({
        ...column,
        type: column.isPrimary
          ? 'objectId'
          : toMongoTargetColumnType(column.type),
        name: column.isPrimary ? '_id' : column.name,
      }));
      const relations = relationsByTable.get(table.name)!.map((relation) => ({
        ...relation,
        mappedBy: relation.mappedById ? 'inverse' : undefined,
        targetTable: tablesById.get(String(relation.targetTableId)),
        foreignKeyColumn: relation.mappedById
          ? catalog.relations.find(
              (entry) => String(entry.id) === String(relation.mappedById),
            )?.propertyName
          : relation.propertyName,
      }));
      const specs = buildMongoFullIndexSpecs({
        collectionName: table.name,
        columns,
        relations,
        uniques: parseJson(table.uniques) ?? [],
        indexes: parseJson(table.indexes) ?? [],
      });
      for (const spec of specs)
        await db.collection(table.name).createIndex(spec.keys, spec.options);
      await db.command({
        collMod: table.name,
        validator: { $jsonSchema: buildMongoValidationSchema(columns) },
        validationLevel: 'moderate',
        validationAction: 'error',
      });
    }
    for (const relation of catalog.relations.filter(
      (entry) => entry.type === 'many-to-many' && !entry.mappedById,
    )) {
      const name = relation.junctionTableName;
      await db.collection(name).createIndex(
        {
          [relation.junctionSourceColumn]: 1,
          [relation.junctionTargetColumn]: 1,
        },
        { name: `${name}_src_tgt_uq`, unique: true },
      );
      await db
        .collection(name)
        .createIndex(
          { [relation.junctionTargetColumn]: 1 },
          { name: `${name}_tgt_idx` },
        );
    }
    writeFileSync(
      join(directory, 'mongodb-import.json'),
      JSON.stringify(
        {
          database,
          counts,
          originalPhysicalNamesRetained: true,
          identityMapping: 'sha256([table, originalId]) first 24 hex',
          temporalPrecision: 'BSON milliseconds',
          metadataProjection: 'shared backend type contract',
        },
        null,
        2,
      ),
      { mode: 0o600 },
    );
  } finally {
    await client.close();
  }
}

async function main() {
  try {
    if (backend === 'mysql') await convertMySql();
    else await convertMongo();
  } finally {
    await source.destroy();
  }
}
if (process.env.CONVERT_IMPORT_ONLY !== '1')
  void main().catch((error) => {
    writeFileSync(
      join(directory, `${backend}-conversion-error.log`),
      String(error.stack ?? error),
      { mode: 0o600 },
    );
    console.error(
      `Conversion failed; private diagnostic: ${backend}-conversion-error.log`,
    );
    process.exitCode = 1;
  });
