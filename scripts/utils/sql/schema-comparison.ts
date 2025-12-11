import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../../../src/infrastructure/knex/utils/naming-helpers';
import {
  ColumnDef,
  RelationDef,
  KnexTableSchema,
} from '../../../src/shared/types/database-init.types';
import { getKnexColumnType } from './schema-parser';

type CurrentSchema = {
  columns: Array<{
    name: string;
    type: string;
    isNullable: boolean;
    defaultValue: any;
    enumValues?: string[] | null;
  }>;
  foreignKeys: Array<{ column: string; references: string; referencesTable: string }>;
  uniques: Array<{ name: string; columns: string[] }>;
  indexes: Array<{ name: string; columns: string[] }>;
};

export async function getCurrentDatabaseSchema(knex: Knex, tableName: string): Promise<CurrentSchema> {
  const dbType = knex.client.config.client;

  if (dbType === 'mysql2') {
    const columnsResult = await knex.raw(`
      SELECT
        COLUMN_NAME as name,
        DATA_TYPE as type,
        IS_NULLABLE as isNullable,
        COLUMN_DEFAULT as defaultValue,
        COLUMN_TYPE as fullType
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND COLUMN_NAME NOT IN ('id', 'createdAt', 'updatedAt')
      ORDER BY ORDINAL_POSITION
    `, [tableName]);

    const fkResult = await knex.raw(`
      SELECT
        COLUMN_NAME as \`column\`,
        REFERENCED_COLUMN_NAME as \`references\`,
        REFERENCED_TABLE_NAME as referencesTable
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND REFERENCED_TABLE_NAME IS NOT NULL
    `, [tableName]);

    const indexResult = await knex.raw(
      `
      SELECT
        INDEX_NAME as indexName,
        NON_UNIQUE as isNonUnique,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = ?
        AND INDEX_NAME != 'PRIMARY'
      GROUP BY INDEX_NAME, NON_UNIQUE
    `,
      [tableName],
    );

    return {
      columns: columnsResult[0].map((col: any) => {
        let enumValues = null;
        if (col.type === 'enum' && col.fullType) {
          const enumMatch = col.fullType.match(/^enum\((.+)\)$/i);
          if (enumMatch) {
            enumValues = enumMatch[1]
              .split(',')
              .map((val: string) => val.trim().replace(/^'|'$/g, ''));
          }
        }
        
        return {
        name: col.name,
          type: col.type === 'enum' ? 'enum' : col.type,
        isNullable: col.isNullable === 'YES',
        defaultValue: col.defaultValue,
          enumValues: enumValues,
        };
      }),
      foreignKeys: fkResult[0],
      uniques: indexResult[0]
        .filter((row: any) => row.isNonUnique === 0)
        .map((row: any) => ({
          name: row.indexName,
          columns: String(row.columns).split(','),
        })),
      indexes: indexResult[0]
        .filter((row: any) => row.isNonUnique === 1)
        .map((row: any) => ({
          name: row.indexName,
          columns: String(row.columns).split(','),
        })),
    };
  } else if (dbType === 'pg') {
    const columnsResult = await knex.raw(`
      SELECT
        c.column_name as name,
        c.data_type as type,
        c.is_nullable as "isNullable",
        c.column_default as "defaultValue",
        CASE 
          WHEN c.data_type = 'USER-DEFINED' AND t.typtype = 'e' THEN
            (SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)
             FROM pg_enum e
             WHERE e.enumtypid = t.oid)
          ELSE NULL
        END as enum_values
      FROM information_schema.columns c
      LEFT JOIN pg_type t ON t.typname = c.udt_name
      WHERE c.table_schema = 'public'
        AND c.table_name = ?
        AND c.column_name NOT IN ('id', 'createdAt', 'updatedAt')
      ORDER BY c.ordinal_position
    `, [tableName]);

    const fkResult = await knex.raw(`
      SELECT
        kcu.column_name as "column",
        ccu.column_name as "references",
        ccu.table_name as "referencesTable"
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
      WHERE tc.table_name = ?
        AND tc.constraint_type = 'FOREIGN KEY'
    `, [tableName]);

    const indexResult = await knex.raw(
      `
      SELECT
        i.relname as index_name,
        ix.indisunique as is_unique,
        array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
      FROM pg_class t
      JOIN pg_index ix ON t.oid = ix.indrelid
      JOIN pg_class i ON i.oid = ix.indexrelid
      JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
      JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
      WHERE t.relname = ?
        AND ix.indisprimary = false
      GROUP BY i.relname, ix.indisunique
    `,
      [tableName],
    );

    return {
      columns: columnsResult.rows.map((col: any) => ({
        name: col.name,
        type: col.type === 'USER-DEFINED' && col.enum_values ? 'enum' : col.type,
        isNullable: col.isNullable === 'YES',
        defaultValue: col.defaultValue,
        enumValues: col.enum_values || null,
      })),
      foreignKeys: fkResult.rows,
      uniques: indexResult.rows
        .filter((row: any) => row.is_unique === true)
        .map((row: any) => ({
          name: row.index_name,
          columns: row.columns || [],
        })),
      indexes: indexResult.rows
        .filter((row: any) => row.is_unique === false)
        .map((row: any) => ({
          name: row.index_name,
          columns: row.columns || [],
        })),
    };
  }

  return { columns: [], foreignKeys: [], uniques: [], indexes: [] };
}

export function compareSchemas(
  snapshotSchema: KnexTableSchema,
  currentSchema: CurrentSchema
): {
  columnsToAdd: ColumnDef[];
  columnsToRemove: string[];
  columnsToModify: Array<{ column: ColumnDef; changes: string[] }>;
  relationsToAdd: RelationDef[];
  relationsToRemove: string[];
  uniquesToAdd: string[][];
  uniquesToRemove: Array<{ columns: string[]; name?: string }>;
  indexesToAdd: string[][];
  indexesToRemove: Array<{ columns: string[]; name?: string }>;
} {
  const diff = {
    columnsToAdd: [] as ColumnDef[],
    columnsToRemove: [] as string[],
    columnsToModify: [] as Array<{ column: ColumnDef; changes: string[] }>,
    relationsToAdd: [] as RelationDef[],
    relationsToRemove: [] as string[],
    uniquesToAdd: [] as string[][],
    uniquesToRemove: [] as Array<{ columns: string[]; name?: string }>,
    indexesToAdd: [] as string[][],
    indexesToRemove: [] as Array<{ columns: string[]; name?: string }>,
  };

  const snapshotColumns = snapshotSchema.definition.columns.filter(
    col => !col.isPrimary && col.name !== 'createdAt' && col.name !== 'updatedAt'
  );

  const snapshotColumnNames = new Set(snapshotColumns.map(c => c.name));
  const currentColumnNamesSet = new Set(currentSchema.columns.map(c => c.name));

  for (const col of snapshotColumns) {
    if (!currentColumnNamesSet.has(col.name)) {
      diff.columnsToAdd.push(col);
    }
  }

  const snapshotFkColumnNames = new Set<string>();
  if (snapshotSchema.definition.relations) {
    for (const rel of snapshotSchema.definition.relations) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        snapshotFkColumnNames.add(fkColumn);
      }
    }
  }

  for (const col of currentSchema.columns) {
    if (snapshotColumnNames.has(col.name)) {
      continue;
    }
    const isCurrentFkColumn = currentSchema.foreignKeys.some(fk => fk.column === col.name);
    if (isCurrentFkColumn && !snapshotFkColumnNames.has(col.name)) {
      continue;
    }
    if (snapshotFkColumnNames.has(col.name)) {
      continue;
    }
    diff.columnsToRemove.push(col.name);
  }

  for (const snapshotCol of snapshotColumns) {
    const currentCol = currentSchema.columns.find(c => c.name === snapshotCol.name);
    if (currentCol) {
      const changes: string[] = [];

      const snapshotType = getKnexColumnType(snapshotCol);
      if (snapshotType !== currentCol.type && !isTypeCompatible(snapshotType, currentCol.type)) {
        changes.push('type');
      }

      if (snapshotCol.type === 'enum' && Array.isArray(snapshotCol.options)) {
        const currentEnumValues = currentCol.enumValues || [];
        const snapshotEnumValues = snapshotCol.options || [];
        const enumValuesMatch = 
          currentEnumValues.length === snapshotEnumValues.length &&
          currentEnumValues.every((val: string, idx: number) => val === snapshotEnumValues[idx]);
        
        if (!enumValuesMatch) {
          changes.push('enum-options');
        }
      }

      const snapshotNullable = snapshotCol.isNullable !== false;
      if (snapshotNullable !== currentCol.isNullable) {
        changes.push('nullable');
      }

      if (changes.length > 0) {
        diff.columnsToModify.push({ column: snapshotCol, changes });
      }
    }
  }

  const snapshotFkColumns = new Set<string>();
  if (snapshotSchema.definition.relations) {
    for (const rel of snapshotSchema.definition.relations) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        snapshotFkColumns.add(getForeignKeyColumnName(rel.propertyName));
      }
    }
  }

  const currentFkColumns = new Set(currentSchema.foreignKeys.map(fk => fk.column));

  const currentColumnNames = new Set(currentSchema.columns.map(c => c.name));

  if (snapshotSchema.definition.relations) {
    for (const rel of snapshotSchema.definition.relations) {
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        if (!currentFkColumns.has(fkColumn) && !currentColumnNames.has(fkColumn)) {
          diff.relationsToAdd.push(rel);
        }
      } else if (rel.type === 'many-to-many') {
        diff.relationsToAdd.push(rel);
      }
    }
  }

  for (const fk of currentSchema.foreignKeys) {
    if (!snapshotFkColumns.has(fk.column)) {
      diff.relationsToRemove.push(fk.column);
    }
  }

  // ---- Uniques / Indexes ----
  const resolveFieldName = (fieldName: string): string => {
    const relation = snapshotSchema.definition.relations?.find(
      (r) => r.propertyName === fieldName,
    );
    if (relation && (relation.type === 'many-to-one' || relation.type === 'one-to-one')) {
      return getForeignKeyColumnName(relation.propertyName);
    }
    return fieldName;
  };

  const normalizeGroup = (cols: string[] | string): string => {
    const arr = Array.isArray(cols) ? cols : String(cols || '').split(',');
    return arr.map((c) => c.trim().toLowerCase()).join('|');
  };

  const snapshotUniqueGroups =
    snapshotSchema.definition.uniques?.map((group) => group.map(resolveFieldName)) || [];
  const snapshotIndexGroups =
    snapshotSchema.definition.indexes?.map((group) => group.map(resolveFieldName)) || [];

  const currentUniqueMap = new Map<string, { columns: string[]; name?: string }>();
  for (const u of currentSchema.uniques || []) {
    const key = normalizeGroup(u.columns || []);
    currentUniqueMap.set(key, { columns: u.columns || [], name: u.name });
  }

  const currentIndexMap = new Map<string, { columns: string[]; name?: string }>();
  for (const idx of currentSchema.indexes || []) {
    const key = normalizeGroup(idx.columns || []);
    currentIndexMap.set(key, { columns: idx.columns || [], name: idx.name });
  }

  // uniques to add
  for (const group of snapshotUniqueGroups) {
    const key = normalizeGroup(group);
    if (!currentUniqueMap.has(key)) {
      diff.uniquesToAdd.push(group);
    }
  }

  // uniques to remove
  for (const [key, info] of currentUniqueMap.entries()) {
    const existsInSnapshot = snapshotUniqueGroups.some(
      (g) => normalizeGroup(g) === key,
    );
    if (!existsInSnapshot) {
      diff.uniquesToRemove.push({ columns: info.columns, name: info.name });
    }
  }

  // indexes to add
  for (const group of snapshotIndexGroups) {
    const key = normalizeGroup(group);
    if (!currentIndexMap.has(key)) {
      diff.indexesToAdd.push(group);
    }
  }

  // indexes to remove
  for (const [key, info] of currentIndexMap.entries()) {
    const existsInSnapshot = snapshotIndexGroups.some(
      (g) => normalizeGroup(g) === key,
    );
    if (!existsInSnapshot) {
      diff.indexesToRemove.push({ columns: info.columns, name: info.name });
    }
  }

  return diff;
}

export function isTypeCompatible(type1: string, type2: string): boolean {
  const compatibleTypes: Record<string, string[]> = {
    'integer': ['int', 'integer', 'bigint', 'smallint', 'tinyint'],
    'string': ['varchar', 'text', 'char'],
    'text': ['varchar', 'text', 'longtext', 'mediumtext'],
    'timestamp': ['timestamp', 'datetime'],
    'boolean': ['tinyint', 'boolean', 'bool'],
  };

  for (const [baseType, variants] of Object.entries(compatibleTypes)) {
    if ((type1 === baseType || variants.includes(type1)) &&
        (type2 === baseType || variants.includes(type2))) {
      return true;
    }
  }

  return type1 === type2;
}

