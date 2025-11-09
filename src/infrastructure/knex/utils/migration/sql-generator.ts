export function generateColumnDefinition(col: any, dbType: 'mysql' | 'postgres' | 'sqlite' = 'mysql'): string {
  let definition = '';

  switch (col.type) {
    case 'uuid':
      if (dbType === 'postgres') {
        definition = 'UUID';
      } else {
        definition = 'VARCHAR(36)';
      }
      break;
    case 'int':
      if (col.isPrimary && col.isGenerated) {
        // Auto-increment primary key
        if (dbType === 'postgres') {
          definition = 'SERIAL';
        } else if (dbType === 'sqlite') {
          definition = 'INTEGER';
        } else {
          definition = 'INT UNSIGNED AUTO_INCREMENT';
        }
      } else {
        // Regular integer
        if (dbType === 'postgres') {
          definition = 'INTEGER';
        } else if (dbType === 'sqlite') {
          definition = 'INTEGER';
        } else {
          definition = 'INT UNSIGNED';
        }
      }
      break;
    case 'bigint':
      if (dbType === 'postgres') {
        definition = 'BIGINT';
      } else if (dbType === 'sqlite') {
        definition = 'INTEGER';
      } else {
        definition = 'BIGINT';
      }
      break;
    case 'varchar':
    case 'text':
      definition = `VARCHAR(${col.options?.length || 255})`;
      break;
    case 'longtext':
      if (dbType === 'postgres') {
        definition = 'TEXT';
      } else if (dbType === 'sqlite') {
        definition = 'TEXT';
      } else {
        definition = 'LONGTEXT';
      }
      break;
    case 'boolean':
      if (dbType === 'postgres') {
        definition = 'BOOLEAN';
      } else if (dbType === 'sqlite') {
        definition = 'INTEGER'; // SQLite stores boolean as 0/1
      } else {
        definition = 'BOOLEAN';
      }
      break;
    case 'datetime':
    case 'timestamp':
      if (dbType === 'postgres') {
        definition = 'TIMESTAMP';
      } else if (dbType === 'sqlite') {
        definition = 'TEXT'; // SQLite stores timestamps as TEXT or INTEGER
      } else {
        definition = 'TIMESTAMP';
      }
      break;
    case 'date':
      if (dbType === 'postgres') {
        definition = 'DATE';
      } else if (dbType === 'sqlite') {
        definition = 'TEXT';
      } else {
        definition = 'DATE';
      }
      break;
    case 'decimal':
      if (dbType === 'sqlite') {
        definition = 'REAL';
      } else {
        definition = `DECIMAL(${col.options?.precision || 10}, ${col.options?.scale || 2})`;
      }
      break;
    case 'json':
    case 'simple-json':
      if (dbType === 'postgres') {
        definition = 'JSONB';
      } else if (dbType === 'sqlite') {
        definition = 'TEXT';
      } else {
        definition = 'JSON';
      }
      break;
    case 'enum':
      if (dbType === 'postgres' && Array.isArray(col.options)) {
        // PostgreSQL uses CHECK constraint for enums in this context
        const enumValues = col.options.map(opt => `'${opt}'`).join(', ');
        definition = `VARCHAR(255) CHECK (${col.name} IN (${enumValues}))`;
      } else if (dbType === 'sqlite') {
        definition = 'TEXT';
      } else if (Array.isArray(col.options)) {
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
  } else if (col.isPrimary && col.isGenerated && dbType === 'sqlite') {
    // SQLite needs explicit PRIMARY KEY for INTEGER autoincrement
    definition += ' PRIMARY KEY AUTOINCREMENT';
  }

  if (col.isNullable === false && !(col.isPrimary && col.isGenerated)) {
    definition += ' NOT NULL';
  }

  if (col.defaultValue !== null && col.defaultValue !== undefined) {
    if (typeof col.defaultValue === 'string') {
      definition += ` DEFAULT '${col.defaultValue}'`;
    } else if (typeof col.defaultValue === 'boolean') {
      if (dbType === 'sqlite') {
        definition += ` DEFAULT ${col.defaultValue ? 1 : 0}`;
      } else {
        definition += ` DEFAULT ${col.defaultValue ? 1 : 0}`;
      }
    } else {
      definition += ` DEFAULT ${col.defaultValue}`;
    }
  }

  return definition;
}

