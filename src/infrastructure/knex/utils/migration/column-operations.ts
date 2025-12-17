import { Knex } from 'knex';

export function addColumnToTable(table: Knex.CreateTableBuilder, col: any): void {
  let column: Knex.ColumnBuilder;

  switch (col.type) {
    case 'uuid':
      column = table.string(col.name, 36);
      if (col.isPrimary) {
        column.primary();
      }
      break;

    case 'int':
      if (col.isPrimary) {
        // Auto increment is default for int primary keys
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
      column = table.decimal(col.name, col.options?.precision || 10, col.options?.scale || 2);
      break;

    case 'simple-json':
      column = table.text(col.name, 'longtext');
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

  if (col.defaultValue !== null && col.defaultValue !== undefined) {
    column.defaultTo(col.defaultValue);
  }
}

export function hasColumnChanged(oldCol: any, newCol: any): boolean {
  return (
    oldCol.type !== newCol.type ||
    oldCol.isNullable !== newCol.isNullable ||
    oldCol.isGenerated !== newCol.isGenerated ||
    JSON.stringify(oldCol.defaultValue) !== JSON.stringify(newCol.defaultValue) ||
    JSON.stringify(oldCol.options) !== JSON.stringify(newCol.options)
  );
}
