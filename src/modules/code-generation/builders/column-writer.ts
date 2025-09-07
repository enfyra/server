import { ClassDeclaration } from 'ts-morph';

interface ColumnWriterContext {
  classDeclaration: ClassDeclaration;
  col: Partial<any>;
  usedImports: Set<string>;
  helpers: {
    capitalize: (s: string) => string;
    dbTypeToTSType: (type: string) => string;
  };
}

export function addColumnToClass({
  classDeclaration,
  col,
  usedImports,
  helpers,
}: ColumnWriterContext): void {
  const decorators: { name: string; arguments: string[] }[] = [];

  if (col.isPrimary) {
    const strategy = col.type === 'uuid' ? `'uuid'` : `'increment'`;
    decorators.push({ name: 'PrimaryGeneratedColumn', arguments: [strategy] });
    usedImports.add('PrimaryGeneratedColumn');
  } else {
    const dbType =
      col.type === 'date'
        ? 'timestamp'
        : col.type === 'richtext' || col.type === 'code'
          ? 'text'
          : col.type === 'array-select'
            ? 'simple-json'
            : col.type;

    const opts = [`type: "${dbType}"`];

    if (col.isNullable === false) {
      opts.push('nullable: false');
    } else {
      opts.push('nullable: true');
    }

    if (col.defaultValue !== undefined && col.defaultValue !== null) {
      const invalidDefault =
        (col.type === 'uuid' && col.defaultValue === '') ||
        (col.type === 'number' && isNaN(Number(col.defaultValue))) ||
        // ✅ MySQL: TEXT, BLOB, GEOMETRY, JSON columns can't have default values
        col.type === 'text' ||
        col.type === 'blob' ||
        col.type === 'longtext' ||
        col.type === 'mediumtext' ||
        col.type === 'tinytext' ||
        col.type === 'longblob' ||
        col.type === 'mediumblob' ||
        col.type === 'tinyblob' ||
        col.type === 'json' ||
        col.type === 'geometry' ||
        col.type === 'point' ||
        col.type === 'linestring' ||
        col.type === 'polygon';

      if (invalidDefault) {
        // skip invalid default value
      } else if (col.defaultValue === 'now') {
        opts.push(`default: () => "now()"`);
      } else {
        opts.push(
          typeof col.defaultValue === 'string'
            ? `default: "${col.defaultValue}"`
            : `default: ${col.defaultValue}`,
        );
      }
    }

    // Skip field-level unique/index - only use class-level constraints
    // if (col.isUnique) opts.push('unique: true');
    
    if (col.type === 'enum' && col.options) {
      opts.push(`enum: [${col.options.map((v) => `'${v}'`).join(', ')}]`);
    }
    if (col.isUpdatable === false) {
      opts.push(`update: false`);
    }

    decorators.push({ name: 'Column', arguments: [`{ ${opts.join(', ')} }`] });
    usedImports.add('Column');

    // Skip field-level index - only use class-level constraints
    // if (col.isIndex) {
    //   decorators.push({ name: 'Index', arguments: [] });
    //   usedImports.add('Index');
    // }
  }

  if (col.isHidden) {
    decorators.push({ name: 'HiddenField', arguments: [] });
    usedImports.add('HiddenField');
  }

  const tsType =
    col.type === 'enum'
      ? col.options.map((v) => `'${v}'`).join(' | ')
      : col.type === 'array-select'
        ? 'any[]'
      : col.type === 'date'
        ? 'Date'
        : col.type === 'richtext' || col.type === 'code'
          ? 'string'
          : helpers.dbTypeToTSType(col.type);

  classDeclaration.addProperty({
    name: col.name,
    type: tsType,
    hasExclamationToken: false,
    decorators,
  });
}
