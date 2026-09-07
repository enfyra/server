import { Knex } from 'knex';
import { supportsSqlColumnDefault } from './sql-generator';
import { addSqlEnumColumn } from '../sql-enum.util';

export function addColumnToTable(
  table: Knex.CreateTableBuilder,
  col: any,
  dbType: 'mysql' | 'postgres' | string = 'mysql',
  tableName?: string,
): void {
  let column: Knex.ColumnBuilder;
  switch (col.type) {
    case 'uuid':
      column = table.uuid(col.name);
      if (col.isPrimary) {
        column.primary();
      }
      break;
    case 'int':
      if (col.isPrimary) {
        column = table.increments(col.name).unsigned();
      } else {
        column = table.integer(col.name);
      }
      break;
    case 'bigint':
      column = table.bigInteger(col.name);
      break;
    case 'richtext':
    case 'code':
      column = table.text(col.name, 'longtext');
      break;
    case 'varchar':
      column = table.string(col.name, col.options?.length || 255);
      break;
    case 'text':
      column = table.text(col.name);
      break;
    case 'longtext':
      column = table.text(col.name, 'longtext');
      break;
    case 'boolean':
      column = table.boolean(col.name);
      break;
    case 'datetime':
      column = table.dateTime(col.name);
      break;
    case 'timestamp':
      column = table.timestamp(col.name);
      break;
    case 'date':
      column = table.date(col.name);
      break;
    case 'decimal':
      column = table.decimal(
        col.name,
        col.options?.precision || 10,
        col.options?.scale || 2,
      );
      break;
    case 'float':
      column = table.float(col.name);
      break;
    case 'simple-json':
      column = table.text(col.name, 'longtext');
      break;
    case 'enum':
      if (
        !tableName ||
        !Array.isArray(col.options) ||
        col.options.length === 0
      ) {
        throw new Error(
          `Enum column ${col.name} requires table identity and options`,
        );
      }
      column = addSqlEnumColumn(
        table,
        tableName,
        col.name,
        col.options,
        dbType,
      );
      break;
    default:
      column = table.string(col.name);
  }
  if (!col.isPrimary) {
    const isNullable = col.isNullable ?? true;
    if (!isNullable) {
      column.notNullable();
    }
  }
  if (
    col.defaultValue !== null &&
    col.defaultValue !== undefined &&
    supportsSqlColumnDefault(col, dbType)
  ) {
    column.defaultTo(col.defaultValue);
  }
}
export function hasColumnChanged(oldCol: any, newCol: any): boolean {
  return (
    oldCol.type !== newCol.type ||
    oldCol.isNullable !== newCol.isNullable ||
    oldCol.isGenerated !== newCol.isGenerated ||
    JSON.stringify(oldCol.defaultValue) !==
      JSON.stringify(newCol.defaultValue) ||
    JSON.stringify(oldCol.options) !== JSON.stringify(newCol.options)
  );
}
