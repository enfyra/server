import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../../../src/infrastructure/knex/utils/naming-helpers';
import { KnexTableSchema } from '../../../src/shared/types/database-init.types';
import { getKnexColumnType, getPrimaryKeyType } from './schema-parser';
import { addForeignKeys } from './foreign-keys';
import { createJunctionTables } from './junction-tables';

export function buildTableSchema(
  table: Knex.CreateTableBuilder,
  schema: KnexTableSchema,
  dbType: string,
  schemas: KnexTableSchema[],
  knex: Knex,
): void {
  const { definition } = schema;

  for (const col of definition.columns) {
    let column: Knex.ColumnBuilder;

    const knexType = getKnexColumnType(col);

    if (col.isPrimary && col.isGenerated) {
      if (col.type === 'uuid') {
        if (dbType === 'postgres') {
          column = table.uuid(col.name).primary().defaultTo(knex.raw('gen_random_uuid()'));
        } else {
          column = table.uuid(col.name).primary();
        }
      } else {
        column = table.increments(col.name).primary();
      }
    }
    else if (col.type === 'enum' && Array.isArray(col.options)) {
      column = table.enum(col.name, col.options);
    }
    else {
      switch (knexType) {
        case 'integer':
          column = table.integer(col.name);
          break;
        case 'bigInteger':
          column = table.bigInteger(col.name);
          break;
        case 'smallint':
          column = table.integer(col.name);
          break;
        case 'string':
          column = table.string(col.name, 255);
          break;
        case 'text':
          column = table.text(col.name);
          break;
        case 'boolean':
          column = table.boolean(col.name);
          break;
        case 'uuid':
          column = table.uuid(col.name);
          break;
        case 'timestamp':
          column = table.timestamp(col.name);
          break;
        case 'datetime':
          column = table.datetime(col.name);
          break;
        case 'simple-json':
          column = table.text(col.name, 'longtext');
          break;
        default:
          column = table.text(col.name);
      }
    }

    if (col.isNullable === false) {
      column.notNullable();
    } else {
      column.nullable();
    }

    if (col.defaultValue !== undefined && col.defaultValue !== null) {
      if (typeof col.defaultValue === 'string' && col.defaultValue.toLowerCase() === 'now') {
        column.defaultTo(knex.fn.now());
      } else {
        if (col.type === 'boolean') {
          let defVal: any = col.defaultValue;
          if (typeof defVal === 'number') defVal = defVal === 1;
          else if (typeof defVal === 'string') {
            const t = defVal.trim().toLowerCase();
            if (t === '1' || t === 'true') defVal = true;
            else if (t === '0' || t === 'false') defVal = false;
          }
          column.defaultTo(!!defVal);
        } else {
          column.defaultTo(col.defaultValue);
        }
      }
    }

    if (col.isUnique) {
      column.unique();
    }

    if (col.description) {
      column.comment(col.description);
    }
  }

  if (definition.relations) {
    for (const relation of definition.relations) {
      if (relation.type === 'many-to-many' || relation.type === 'one-to-many') {
        continue;
      }

      if (relation.type === 'one-to-one' && (relation as any)._isInverseGenerated) {
        continue;
      }

      const foreignKeyColumn = getForeignKeyColumnName(relation.propertyName);

      const targetPkType = getPrimaryKeyType(schemas, relation.targetTable);

      let col;
      if (targetPkType === 'uuid') {
        if (dbType === 'postgres') {
          col = table.uuid(foreignKeyColumn);
        } else {
          col = table.string(foreignKeyColumn, 36);
        }
      } else {
        col = table.integer(foreignKeyColumn).unsigned();
      }

      if (relation.isNullable === false) {
        col.notNullable();
      } else {
        col.nullable();
      }
    }
  }

  if (dbType === 'postgres') {
    table.timestamp('createdAt', { useTz: true }).defaultTo(knex.fn.now());
    table.timestamp('updatedAt', { useTz: true }).defaultTo(knex.fn.now());
  } else {
    table.timestamp('createdAt').defaultTo(knex.fn.now());
    table.timestamp('updatedAt').defaultTo(knex.fn.now());
  }

  if (definition.uniques && definition.uniques.length > 0) {
    for (const uniqueGroup of definition.uniques) {
      if (Array.isArray(uniqueGroup) && uniqueGroup.length > 0) {
        const columnNames = uniqueGroup.map((fieldName) => {
          const relation = definition.relations?.find(r => r.propertyName === fieldName);
          if (relation) {
            return getForeignKeyColumnName(relation.propertyName);
          }
          return fieldName;
        });
        table.unique(columnNames);
      }
    }
  }

  if (definition.indexes && definition.indexes.length > 0) {
    for (const indexGroup of definition.indexes) {
      if (Array.isArray(indexGroup) && indexGroup.length > 0) {
        const columnNames = indexGroup.map((fieldName) => {
          const relation = definition.relations?.find(r => r.propertyName === fieldName);
          if (relation) {
            return getForeignKeyColumnName(relation.propertyName);
          }
          return fieldName;
        });
        table.index(columnNames);
      }
    }
  }

  table.index(['createdAt']);
  table.index(['updatedAt']);
  table.index(['createdAt', 'updatedAt']);

  const timestampFields = definition.columns.filter(col =>
    col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date'
  );

  for (const field of timestampFields) {
    table.index([field.name]);
  }
}

export async function createTable(
  knex: Knex,
  schema: KnexTableSchema,
  dbType: string,
  schemas: KnexTableSchema[],
): Promise<void> {
  const { tableName } = schema;

  console.log(`📝 Creating table: ${tableName}`);

  await knex.schema.createTable(tableName, (table) => {
    buildTableSchema(table, schema, dbType, schemas, knex);
  });

  console.log(`✅ Created table: ${tableName}`);
}

export async function createAllTables(
  knex: Knex,
  schemas: KnexTableSchema[],
  dbType: string,
): Promise<void> {
  console.log('🚀 Creating all tables...');
  
  for (const schema of schemas) {
    const exists = await knex.schema.hasTable(schema.tableName);
    if (!exists) {
      await createTable(knex, schema, dbType, schemas);
    } else {
      console.log(`⏩ Table already exists: ${schema.tableName}`);
    }
  }

  await addForeignKeys(knex, schemas, dbType);

  await createJunctionTables(knex, schemas);

  console.log('✅ All tables created successfully!');
}

