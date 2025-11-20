import { Knex } from 'knex';
import { getForeignKeyColumnName } from '../../../src/infrastructure/knex/utils/naming-helpers';
import {
  ColumnDef,
  RelationDef,
  KnexTableSchema,
} from '../../../src/shared/types/database-init.types';
import { getKnexColumnType } from './schema-parser';

export async function getCurrentDatabaseSchema(knex: Knex, tableName: string): Promise<{
  columns: Array<{ name: string; type: string; isNullable: boolean; defaultValue: any }>;
  foreignKeys: Array<{ column: string; references: string; referencesTable: string }>;
}> {
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

    return {
      columns: columnsResult.rows.map((col: any) => ({
        name: col.name,
        type: col.type === 'USER-DEFINED' && col.enum_values ? 'enum' : col.type,
        isNullable: col.isNullable === 'YES',
        defaultValue: col.defaultValue,
        enumValues: col.enum_values || null,
      })),
      foreignKeys: fkResult.rows,
    };
  }

  return { columns: [], foreignKeys: [] };
}

export function compareSchemas(
  snapshotSchema: KnexTableSchema,
  currentSchema: { columns: any[]; foreignKeys: any[] }
): {
  columnsToAdd: ColumnDef[];
  columnsToRemove: string[];
  columnsToModify: Array<{ column: ColumnDef; changes: string[] }>;
  relationsToAdd: RelationDef[];
  relationsToRemove: string[];
} {
  const diff = {
    columnsToAdd: [] as ColumnDef[],
    columnsToRemove: [] as string[],
    columnsToModify: [] as Array<{ column: ColumnDef; changes: string[] }>,
    relationsToAdd: [] as RelationDef[],
    relationsToRemove: [] as string[],
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

