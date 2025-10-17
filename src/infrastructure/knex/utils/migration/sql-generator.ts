export function generateColumnDefinition(col: any): string {
  let definition = '';

  switch (col.type) {
    case 'uuid':
      definition = 'VARCHAR(36)';
      break;
    case 'int':
      if (col.isPrimary && col.isGenerated) {
        definition = 'INT UNSIGNED AUTO_INCREMENT';
      } else {
        definition = 'INT UNSIGNED';
      }
      break;
    case 'bigint':
      definition = 'BIGINT';
      break;
    case 'varchar':
    case 'text':
      definition = `VARCHAR(${col.options?.length || 255})`;
      break;
    case 'longtext':
      definition = 'LONGTEXT';
      break;
    case 'boolean':
      definition = 'BOOLEAN';
      break;
    case 'datetime':
    case 'timestamp':
      definition = 'TIMESTAMP';
      break;
    case 'date':
      definition = 'DATE';
      break;
    case 'decimal':
      definition = `DECIMAL(${col.options?.precision || 10}, ${col.options?.scale || 2})`;
      break;
    case 'json':
    case 'simple-json':
      definition = 'JSON';
      break;
    case 'enum':
      if (Array.isArray(col.options)) {
        const enumValues = col.options.map(opt => `'${opt}'`).join(', ');
        definition = `ENUM(${enumValues})`;
      } else {
        definition = 'VARCHAR(255)';
      }
      break;
    default:
      definition = 'VARCHAR(255)';
  }

  if (col.isPrimary && !col.isGenerated) {
    definition += ' PRIMARY KEY';
  }

  if (col.isNullable === false) {
    definition += ' NOT NULL';
  }

  if (col.defaultValue !== null && col.defaultValue !== undefined) {
    if (typeof col.defaultValue === 'string') {
      definition += ` DEFAULT '${col.defaultValue}'`;
    } else if (typeof col.defaultValue === 'boolean') {
      definition += ` DEFAULT ${col.defaultValue ? 1 : 0}`;
    } else {
      definition += ` DEFAULT ${col.defaultValue}`;
    }
  }

  return definition;
}

export function generateAddColumnSQL(tableName: string, col: any): string[] {
  const sqlStatements: string[] = [];
  const columnDef = generateColumnDefinition(col);
  sqlStatements.push(`ALTER TABLE \`${tableName}\` ADD COLUMN \`${col.name}\` ${columnDef}`);

  if (col.isForeignKey && col.foreignKeyTarget) {
    const onDelete = col.isNullable !== false ? 'SET NULL' : 'RESTRICT';
    sqlStatements.push(
      `ALTER TABLE \`${tableName}\` ADD CONSTRAINT \`fk_${tableName}_${col.name}\` FOREIGN KEY (\`${col.name}\`) REFERENCES \`${col.foreignKeyTarget}\` (\`${col.foreignKeyColumn || 'id'}\`) ON DELETE ${onDelete} ON UPDATE CASCADE`
    );
  }

  return sqlStatements;
}

export function generateModifyColumnSQL(tableName: string, col: any): string {
  const columnDef = generateColumnDefinition(col);
  return `ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${col.name}\` ${columnDef}`;
}

export function generateDropColumnSQL(tableName: string, columnName: string): string {
  return `ALTER TABLE \`${tableName}\` DROP COLUMN \`${columnName}\``;
}

export function generateRenameColumnSQL(tableName: string, oldName: string, newName: string): string {
  return `ALTER TABLE \`${tableName}\` RENAME COLUMN \`${oldName}\` TO \`${newName}\``;
}

export function generateRenameTableSQL(oldName: string, newName: string): string {
  return `ALTER TABLE \`${oldName}\` RENAME TO \`${newName}\``;
}

export function generateAddUniqueSQL(tableName: string, columns: string[]): string {
  const cols = columns.map((col: string) => `\`${col}\``).join(', ');
  return `ALTER TABLE \`${tableName}\` ADD UNIQUE (${cols})`;
}

export function generateAddIndexSQL(tableName: string, columns: string[]): string {
  const cols = columns.map((col: string) => `\`${col}\``).join(', ');
  const indexName = `idx_${tableName}_${columns.join('_')}`;
  return `ALTER TABLE \`${tableName}\` ADD INDEX \`${indexName}\` (${cols})`;
}
