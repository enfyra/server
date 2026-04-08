import { Injectable, Logger } from '@nestjs/common';
import { KnexService } from '../knex.service';

@Injectable()
export class DatabaseSchemaService {
  private readonly logger = new Logger(DatabaseSchemaService.name);

  constructor(private readonly knexService: KnexService) {}

  async getAllTableSchemas(): Promise<Map<string, any>> {
    const knex = this.knexService.getKnex();
    const dbType = process.env.DB_TYPE;

    if (dbType === 'mysql') {
      return this.getAllMySQLTableSchemas(knex);
    } else if (dbType === 'postgres') {
      return this.getAllPostgreSQLTableSchemas(knex);
    }
    return new Map();
  }

  private async getAllMySQLTableSchemas(knex: any): Promise<Map<string, any>> {
    const db = knex.client.database();
    const [allTables, allColumns, allIndexes, allForeignKeys] = await Promise.all([
      knex('INFORMATION_SCHEMA.TABLES').select('TABLE_NAME', 'TABLE_COMMENT').where('TABLE_SCHEMA', db),
      knex('INFORMATION_SCHEMA.COLUMNS').select('TABLE_NAME', 'COLUMN_NAME as name', 'DATA_TYPE as type', 'IS_NULLABLE as isNullable', 'COLUMN_DEFAULT as defaultValue', 'COLUMN_KEY as columnKey', 'EXTRA as extra', 'COLUMN_COMMENT as description', 'CHARACTER_MAXIMUM_LENGTH as maxLength', 'NUMERIC_PRECISION as precision', 'NUMERIC_SCALE as scale').where('TABLE_SCHEMA', db).orderBy('ORDINAL_POSITION'),
      knex('INFORMATION_SCHEMA.STATISTICS').select('TABLE_NAME', 'INDEX_NAME', 'COLUMN_NAME', 'NON_UNIQUE').where('TABLE_SCHEMA', db).where('INDEX_NAME', '!=', 'PRIMARY').orderBy('INDEX_NAME', 'SEQ_IN_INDEX'),
      knex('INFORMATION_SCHEMA.KEY_COLUMN_USAGE').select('TABLE_NAME', 'COLUMN_NAME', 'REFERENCED_TABLE_NAME', 'REFERENCED_COLUMN_NAME', 'CONSTRAINT_NAME').where('TABLE_SCHEMA', db).whereNotNull('REFERENCED_TABLE_NAME'),
    ]);

    const result = new Map<string, any>();
    const tableNames = new Set<string>(allTables.map((t: any) => t.TABLE_NAME));

    for (const tableName of tableNames) {
      const cols = allColumns.filter((c: any) => c.TABLE_NAME === tableName);
      const transformedColumns = cols.map((col: any) => ({
        name: col.name, type: this.mapMySQLDataType(col.type, col), isPrimary: col.columnKey === 'PRI',
        isGenerated: col.extra?.includes('auto_increment') || false, isNullable: col.isNullable === 'YES',
        isSystem: this.isSystemColumn(col.name), isUpdatable: !col.extra?.includes('auto_increment'),
        isHidden: false, defaultValue: col.defaultValue, description: col.description,
        options: { length: col.maxLength, precision: col.precision, scale: col.scale },
      }));

      const idxs = allIndexes.filter((i: any) => i.TABLE_NAME === tableName);
      const indexGroups = this.groupMySQLIndexes(idxs);

      const fks = allForeignKeys.filter((f: any) => f.TABLE_NAME === tableName);
      const relations = this.transformForeignKeysToRelations(fks);

      result.set(tableName, { name: tableName, isSystem: false, uniques: indexGroups.uniques, indexes: indexGroups.indexes, columns: transformedColumns, relations });
    }
    return result;
  }

  private async getAllPostgreSQLTableSchemas(knex: any): Promise<Map<string, any>> {
    const [allTables, allColumns, allPrimaryKeys, allIndexes] = await Promise.all([
      knex('information_schema.tables').select('table_name').where('table_schema', 'public'),
      knex('information_schema.columns').select('table_name', 'column_name as name', 'data_type as type', 'is_nullable as isNullable', 'column_default as defaultValue', 'character_maximum_length as maxLength', 'numeric_precision as precision', 'numeric_scale as scale').where('table_schema', 'public').orderBy('ordinal_position'),
      knex('information_schema.table_constraints').join('information_schema.key_column_usage', function() { this.on('table_constraints.constraint_name', '=', 'key_column_usage.constraint_name').andOn('table_constraints.table_schema', '=', 'key_column_usage.table_schema'); }).select('table_constraints.table_name', 'key_column_usage.column_name').where('table_constraints.table_schema', 'public').where('table_constraints.constraint_type', 'PRIMARY KEY'),
      knex('pg_indexes').select('tablename', 'indexname', 'indexdef').where('schemaname', 'public'),
    ]);

    const result = new Map<string, any>();
    const tableNames = new Set<string>(allTables.map((t: any) => t.table_name));

    for (const tableName of tableNames) {
      const cols = allColumns.filter((c: any) => c.table_name === tableName);
      const pks = new Set(allPrimaryKeys.filter((p: any) => p.table_name === tableName).map((p: any) => p.column_name));

      const transformedColumns = cols.map((col: any) => ({
        name: col.name, type: this.mapPostgreSQLDataType(col.type, col),
        isPrimary: pks.has(col.name), isGenerated: col.defaultValue?.includes('nextval') || false,
        isNullable: col.isNullable === 'YES', isSystem: this.isSystemColumn(col.name),
        isUpdatable: true, isHidden: false, defaultValue: col.defaultValue, description: null,
        options: { length: col.maxLength, precision: col.precision, scale: col.scale },
      }));

      const idxs = allIndexes.filter((i: any) => i.tablename === tableName);
      const uniques: string[][] = [];
      const regularIndexes: string[][] = [];
      for (const idx of idxs) {
        if (idx.indexname.endsWith('_pkey') || idx.indexname === `pk_${tableName}`) continue;
        const isUnique = idx.indexdef?.includes('UNIQUE') || idx.indexname.includes('_unique');
        const columnsMatch = idx.indexdef?.match(/\(([^)]+)\)/);
        if (columnsMatch) {
          const columns = columnsMatch[1].split(',').map((c: string) => c.trim().replace(/"/g, ''));
          if (isUnique) uniques.push(columns); else regularIndexes.push(columns);
        }
      }

      result.set(tableName, { name: tableName, isSystem: false, uniques, indexes: regularIndexes, columns: transformedColumns, relations: [] });
    }
    return result;
  }

  async getActualTableSchema(tableName: string): Promise<any> {
    const knex = this.knexService.getKnex();
    const dbType = process.env.DB_TYPE || 'mysql';

    if (dbType === 'mysql') {
      return await this.getMySQLTableSchema(tableName, knex);
    } else if (dbType === 'postgres') {
      return await this.getPostgreSQLTableSchema(tableName, knex);
    } else {
      throw new Error(`Unsupported database type: ${dbType}`);
    }
  }

  private async getMySQLTableSchema(tableName: string, knex: any): Promise<any> {
    const tableInfo = await knex('INFORMATION_SCHEMA.TABLES')
      .select('TABLE_NAME', 'TABLE_COMMENT')
      .where('TABLE_SCHEMA', knex.client.database())
      .where('TABLE_NAME', tableName)
      .first();

    if (!tableInfo) {
      return null;
    }

    const columns = await knex('INFORMATION_SCHEMA.COLUMNS')
      .select([
        'COLUMN_NAME as name',
        'DATA_TYPE as type',
        'IS_NULLABLE as isNullable',
        'COLUMN_DEFAULT as defaultValue',
        'COLUMN_KEY as columnKey',
        'EXTRA as extra',
        'COLUMN_COMMENT as description',
        'CHARACTER_MAXIMUM_LENGTH as maxLength',
        'NUMERIC_PRECISION as precision',
        'NUMERIC_SCALE as scale'
      ])
      .where('TABLE_SCHEMA', knex.client.database())
      .where('TABLE_NAME', tableName)
      .orderBy('ORDINAL_POSITION');

    const transformedColumns = columns.map(col => ({
      name: col.name,
      type: this.mapMySQLDataType(col.type, col),
      isPrimary: col.columnKey === 'PRI',
      isGenerated: col.extra?.includes('auto_increment') || false,
      isNullable: col.isNullable === 'YES',
      isSystem: this.isSystemColumn(col.name),
      isUpdatable: !col.extra?.includes('auto_increment'),
      isHidden: false,
      defaultValue: col.defaultValue,
      description: col.description,
      options: {
        length: col.maxLength,
        precision: col.precision,
        scale: col.scale
      }
    }));

    const indexes = await knex('INFORMATION_SCHEMA.STATISTICS')
      .select('INDEX_NAME', 'COLUMN_NAME', 'NON_UNIQUE')
      .where('TABLE_SCHEMA', knex.client.database())
      .where('TABLE_NAME', tableName)
      .where('INDEX_NAME', '!=', 'PRIMARY')
      .orderBy('INDEX_NAME', 'SEQ_IN_INDEX');

    const indexGroups = this.groupMySQLIndexes(indexes);

    const foreignKeys = await knex('INFORMATION_SCHEMA.KEY_COLUMN_USAGE')
      .select([
        'COLUMN_NAME',
        'REFERENCED_TABLE_NAME',
        'REFERENCED_COLUMN_NAME',
        'CONSTRAINT_NAME'
      ])
      .where('TABLE_SCHEMA', knex.client.database())
      .where('TABLE_NAME', tableName)
      .whereNotNull('REFERENCED_TABLE_NAME');

    const relations = this.transformForeignKeysToRelations(foreignKeys);

    return {
      name: tableName,
      isSystem: false,
      uniques: indexGroups.uniques,
      indexes: indexGroups.indexes,
      columns: transformedColumns,
      relations
    };
  }

  private async getPostgreSQLTableSchema(tableName: string, knex: any): Promise<any> {
    const tableInfo = await knex('information_schema.tables')
      .select('table_name')
      .where('table_schema', 'public')
      .where('table_name', tableName)
      .first();

    if (!tableInfo) {
      return null;
    }

    const columns = await knex('information_schema.columns')
      .select([
        'column_name as name',
        'data_type as type',
        'is_nullable as isNullable',
        'column_default as defaultValue',
        'character_maximum_length as maxLength',
        'numeric_precision as precision',
        'numeric_scale as scale'
      ])
      .where('table_schema', 'public')
      .where('table_name', tableName)
      .orderBy('ordinal_position');

    const transformedColumns = columns.map(col => ({
      name: col.name,
      type: this.mapPostgreSQLDataType(col.type, col),
      isPrimary: false,
      isGenerated: col.defaultValue?.includes('nextval') || false,
      isNullable: col.isNullable === 'YES',
      isSystem: this.isSystemColumn(col.name),
      isUpdatable: true,
      isHidden: false,
      defaultValue: col.defaultValue,
      description: null,
      options: {
        length: col.maxLength,
        precision: col.precision,
        scale: col.scale
      }
    }));

    const primaryKeys = await knex('information_schema.table_constraints')
      .join('information_schema.key_column_usage', function() {
        this.on('table_constraints.constraint_name', '=', 'key_column_usage.constraint_name')
          .andOn('table_constraints.table_schema', '=', 'key_column_usage.table_schema');
      })
      .select('key_column_usage.column_name')
      .where('table_constraints.table_schema', 'public')
      .where('table_constraints.table_name', tableName)
      .where('table_constraints.constraint_type', 'PRIMARY KEY');

    primaryKeys.forEach(pk => {
      const col = transformedColumns.find(c => c.name === pk.column_name);
      if (col) {
        col.isPrimary = true;
      }
    });

    const indexes = await knex('pg_indexes')
      .select('indexname', 'indexdef')
      .where('tablename', tableName)
      .where('schemaname', 'public');

    // Parse uniques and indexes from pg_indexes
    const uniques: string[][] = [];
    const regularIndexes: string[][] = [];

    for (const idx of indexes) {
      // Skip primary key indexes
      if (idx.indexname.endsWith('_pkey') || idx.indexname === `pk_${tableName}`) {
        continue;
      }

      // Check if unique by indexdef containing 'UNIQUE'
      const isUnique = idx.indexdef?.includes('UNIQUE') || idx.indexname.includes('_unique');

      // Extract column names from indexdef
      // Format: CREATE UNIQUE INDEX "indexname" ON "tablename" ("col1", "col2")
      const columnsMatch = idx.indexdef?.match(/\(([^)]+)\)/);
      if (columnsMatch) {
        const columns = columnsMatch[1]
          .split(',')
          .map((col: string) => col.trim().replace(/"/g, ''));

        if (isUnique) {
          uniques.push(columns);
        } else {
          regularIndexes.push(columns);
        }
      }
    }

    return {
      name: tableName,
      isSystem: false,
      uniques,
      indexes: regularIndexes,
      columns: transformedColumns,
      relations: []
    };
  }

  private mapMySQLDataType(mysqlType: string, col: any): string {
    const type = mysqlType.toLowerCase();
    
    if (type.includes('int')) return 'int';
    if (type.includes('bigint')) return 'bigint';
    if (type.includes('varchar') || type.includes('char')) return 'varchar';
    if (type.includes('text')) return 'text';
    if (type.includes('longtext')) return 'longtext';
    if (type.includes('decimal') || type.includes('numeric')) return 'decimal';
    if (type.includes('datetime') || type.includes('timestamp')) return 'datetime';
    if (type.includes('date')) return 'date';
    if (type.includes('json')) return 'json';
    if (type.includes('enum')) return 'enum';
    if (type.includes('boolean') || type.includes('tinyint(1)')) return 'boolean';

    return 'varchar';
  }

  private mapPostgreSQLDataType(pgType: string, col: any): string {
    const type = pgType.toLowerCase();
    
    if (type.includes('integer') || type.includes('int4')) return 'int';
    if (type.includes('bigint') || type.includes('int8')) return 'bigint';
    if (type.includes('varchar') || type.includes('character varying')) return 'varchar';
    if (type.includes('text')) return 'text';
    if (type.includes('numeric') || type.includes('decimal')) return 'decimal';
    if (type.includes('timestamp')) return 'datetime';
    if (type.includes('date')) return 'date';
    if (type.includes('json') || type.includes('jsonb')) return 'json';
    if (type.includes('boolean') || type.includes('bool')) return 'boolean';
    if (type.includes('uuid')) return 'uuid';

    return 'varchar';
  }

  private groupMySQLIndexes(indexes: any[]): { uniques: string[][], indexes: string[][] } {
    const indexMap = new Map<string, string[]>();
    const uniqueMap = new Map<string, string[]>();

    indexes.forEach(idx => {
      if (idx.NON_UNIQUE === 0) {
        if (!uniqueMap.has(idx.INDEX_NAME)) {
          uniqueMap.set(idx.INDEX_NAME, []);
        }
        uniqueMap.get(idx.INDEX_NAME)!.push(idx.COLUMN_NAME);
      } else {
        if (!indexMap.has(idx.INDEX_NAME)) {
          indexMap.set(idx.INDEX_NAME, []);
        }
        indexMap.get(idx.INDEX_NAME)!.push(idx.COLUMN_NAME);
      }
    });

    return {
      uniques: Array.from(uniqueMap.values()),
      indexes: Array.from(indexMap.values())
    };
  }

  private transformForeignKeysToRelations(foreignKeys: any[]): any[] {
    const relations: any[] = [];
    const fkMap = new Map<string, any>();

    foreignKeys.forEach(fk => {
      const key = fk.COLUMN_NAME;
      if (!fkMap.has(key)) {
        fkMap.set(key, {
          propertyName: this.getPropertyNameFromColumn(fk.COLUMN_NAME),
          type: 'many-to-one',
          targetTable: fk.REFERENCED_TABLE_NAME,
          isNullable: true,
          isSystem: false,
          foreignKeyColumn: fk.COLUMN_NAME
        });
      }
    });

    return Array.from(fkMap.values());
  }

  private getPropertyNameFromColumn(columnName: string): string {
    const suffixes = ['Id', '_id', 'ID'];
    let propertyName = columnName;

    for (const suffix of suffixes) {
      if (propertyName.endsWith(suffix)) {
        propertyName = propertyName.slice(0, -suffix.length);
        break;
      }
    }

    return propertyName;
  }

  private isSystemColumn(columnName: string): boolean {
    const systemColumns = ['id', 'createdAt', 'updatedAt'];
    return systemColumns.includes(columnName);
  }
}
