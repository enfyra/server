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
      if (col.isPrimary) {
        // Auto-increment is default for int primary keys
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
    case 'richtext':
    case 'code':
      if (dbType === 'postgres' || dbType === 'sqlite') {
        definition = 'TEXT';
      } else {
        definition = 'LONGTEXT';
      }
      break;
    case 'varchar':
      definition = `VARCHAR(${col.options?.length || 255})`;
      break;
    case 'text':
      definition = 'TEXT';
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

  if (col.isPrimary) {
    // For int type, auto increment is already handled above (SERIAL, AUTO_INCREMENT)
    // For uuid type, add PRIMARY KEY constraint
    if (col.type === 'uuid') {
      definition += ' PRIMARY KEY';
    } else if (col.type === 'int' && dbType === 'sqlite') {
      // SQLite needs explicit PRIMARY KEY AUTOINCREMENT for integer primary keys
      definition += ' PRIMARY KEY AUTOINCREMENT';
    }
    // For MySQL/PostgreSQL int primary key, AUTO_INCREMENT/SERIAL already implies PRIMARY KEY
  }

  if (col.isNullable === false && !col.isPrimary) {
    // Primary keys are implicitly NOT NULL
    definition += ' NOT NULL';
  }

  if (col.defaultValue !== null && col.defaultValue !== undefined) {
    const isBooleanColumn = String(col.type).toLowerCase() === 'boolean';
    let defaultVal = col.defaultValue;

    // Normalize numeric/string 0/1 to boolean when column is boolean
    if (isBooleanColumn) {
      if (typeof defaultVal === 'number') {
        defaultVal = defaultVal === 1;
      } else if (typeof defaultVal === 'string') {
        const trimmed = defaultVal.trim().toLowerCase();
        if (trimmed === '1' || trimmed === 'true') defaultVal = true;
        else if (trimmed === '0' || trimmed === 'false') defaultVal = false;
      }
    }

    if (typeof defaultVal === 'string' && !isBooleanColumn) {
      definition += ` DEFAULT '${defaultVal}'`;
    } else if (typeof defaultVal === 'boolean') {
      if (dbType === 'postgres') {
        definition += ` DEFAULT ${defaultVal ? 'true' : 'false'}`;
      } else {
        definition += ` DEFAULT ${defaultVal ? 1 : 0}`;
      }
    } else {
      definition += ` DEFAULT ${defaultVal}`;
    }
  }

  return definition;
}

