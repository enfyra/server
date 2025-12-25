import { DatabaseType } from '../../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../knex/utils/naming-helpers';
import {
  quoteIdentifier,
  getJsonObjectFunc,
  getJsonArrayAggFunc,
  getEmptyJsonArray,
  castToText,
} from '../../../knex/utils/migration/sql-dialect';

interface TableMetadata {
  name: string;
  columns: Array<{ name: string; type: string }>;
  relations: Array<{
    propertyName: string;
    type: 'many-to-one' | 'one-to-many' | 'one-to-one' | 'many-to-many';
    targetTableName: string;
    inversePropertyName?: string;
    foreignKeyColumn?: string;
    junctionTableName?: string;
    junctionSourceColumn?: string;
    junctionTargetColumn?: string;
  }>;
}

export async function buildNestedSubquery(
  parentTable: string,
  parentMeta: TableMetadata,
  relationName: string,
  nestedFields: string[],
  dbType: DatabaseType,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
  nestingLevel: number = 0,
  parentAliasOverride?: string,
): Promise<string | null> {
  const relation = parentMeta.relations?.find(r => r.propertyName === relationName);
  if (!relation) {
    console.warn(`[NESTED-SUBQUERY] Relation ${relationName} not found in ${parentTable}`);
    return null;
  }

  const targetMeta = await metadataGetter(relation.targetTableName);
  if (!targetMeta) {
    console.warn(`[NESTED-SUBQUERY] Target metadata not found for ${relation.targetTableName}`);
    return null;
  }

  const rootFields: string[] = [];
  const subRelations = new Map<string, string[]>();

  for (const field of nestedFields) {
    if (field === '*' || !field.includes('.')) {
      rootFields.push(field);
    } else {
      const parts = field.split('.');
      const subRelName = parts[0];
      const remaining = parts.slice(1).join('.');

      if (!subRelations.has(subRelName)) {
        subRelations.set(subRelName, []);
      }
      subRelations.get(subRelName)!.push(remaining);
    }
  }

  const currentAlias = nestingLevel === 0 ? 'c' : `c${nestingLevel}`;
  const parentAlias = parentAliasOverride || (nestingLevel <= 1 ? 'c' : `c${nestingLevel - 1}`);

  const columns: string[] = [];

  if (rootFields.includes('*')) {
    const fkColumnsToOmit = new Set<string>();
    for (const rel of targetMeta.relations || []) {
      if (rel.type === 'many-to-one' || (rel.type === 'one-to-one' && !(rel as any).isInverse)) {
        const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.targetTableName);
        if (fkCol) fkColumnsToOmit.add(fkCol);
      }
    }
    for (const col of targetMeta.columns) {
      if (fkColumnsToOmit.has(col.name)) continue;
      columns.push(`'${col.name}', ${currentAlias}.${quoteIdentifier(col.name, dbType)}`);
    }

    for (const rel of targetMeta.relations || []) {
      if (!subRelations.has(rel.propertyName)) {
        subRelations.set(rel.propertyName, ['id']);
      }
    }
  } else {
    for (const field of rootFields) {
      if (!field.includes('.')) {
        const col = targetMeta.columns.find(c => c.name === field);
        if (col) {
          columns.push(`'${col.name}', ${currentAlias}.${quoteIdentifier(col.name, dbType)}`);
        }
      }
    }
  }

  for (const [subRelName, subFields] of subRelations.entries()) {
    const nestedSubquery = await buildNestedSubquery(
      relation.targetTableName,
      targetMeta,
      subRelName,
      subFields,
      dbType,
      metadataGetter,
      sortOptions.filter(s => s.field.startsWith(`${relationName}.${subRelName}`)),
      nestingLevel + 1,
      parentAliasOverride,
    );

    if (nestedSubquery) {
      columns.push(`'${subRelName}', ${nestedSubquery}`);
    }
  }

  if (columns.length === 0) {
    return null;
  }

  const relSort = sortOptions.find(s => s.field === relationName || s.field.startsWith(relationName + '.'));
  const sortField = relSort?.field.split('.').pop() || '';

  const jsonObjectFunc = getJsonObjectFunc(dbType);
  const jsonObject = `${jsonObjectFunc}(${columns.join(',')})`;

  const nextAlias = nestingLevel === 0 ? 'c' : `c${nestingLevel}`;

  const parentRef = nestingLevel === 0 ? quoteIdentifier(parentTable, dbType) : parentAlias;

  if (relation.type === 'many-to-one' || (relation.type === 'one-to-one' && !(relation as any).isInverse)) {
    const fkColumn = relation.foreignKeyColumn || `${relationName}Id`;
    const leftSide = castToText(`${nextAlias}.id`, dbType);
    const rightSide = castToText(`${parentRef}.${quoteIdentifier(fkColumn, dbType)}`, dbType);
    return `(select ${jsonObject} from ${quoteIdentifier(relation.targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide} limit 1)`;
  } else if (relation.type === 'one-to-one' && (relation as any).isInverse) {
    const fkColumn = relation.foreignKeyColumn || getForeignKeyColumnName(relation.inversePropertyName || relationName);
    const leftSide = castToText(`${nextAlias}.${quoteIdentifier(fkColumn, dbType)}`, dbType);
    const rightSide = castToText(`${parentRef}.id`, dbType);
    return `(select ${jsonObject} from ${quoteIdentifier(relation.targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide} limit 1)`;
  } else if (relation.type === 'one-to-many') {
    let fkColumn = relation.foreignKeyColumn;

    if (!fkColumn) {
      if (relation.inversePropertyName) {
        fkColumn = getForeignKeyColumnName(relation.inversePropertyName);
      } else {
        const inverseRelation = targetMeta.relations?.find(
          r => r.type === 'many-to-one' && r.targetTableName === parentTable
        );
        if (inverseRelation?.foreignKeyColumn) {
          fkColumn = inverseRelation.foreignKeyColumn;
      } else {
        console.warn(`[NESTED-SUBQUERY] O2M relation ${relationName} missing both foreignKeyColumn and inversePropertyName in metadata`);
        return null;
        }
      }
    }

    const orderClause = sortField ? ` order by ${nextAlias}.${quoteIdentifier(sortField, dbType)} ${relSort!.direction.toUpperCase()}` : '';
    const jsonArrayAgg = getJsonArrayAggFunc(dbType);
    const emptyArray = getEmptyJsonArray(dbType);
    const leftSide = castToText(`${nextAlias}.${quoteIdentifier(fkColumn, dbType)}`, dbType);
    const rightSide = castToText(`${parentRef}.id`, dbType);
    return `(select ${jsonArrayAgg}(${jsonObject}), ${emptyArray}) from ${quoteIdentifier(relation.targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide}${orderClause})`;
  } else if (relation.type === 'many-to-many') {
    const junctionTable = relation.junctionTableName;
    if (!junctionTable) {
      console.warn(`[NESTED-SUBQUERY] M2M relation ${relationName} missing junctionTableName`);
      return null;
    }

    const junctionSourceCol = relation.junctionSourceColumn || getForeignKeyColumnName(parentTable);
    const junctionTargetCol = relation.junctionTargetColumn || getForeignKeyColumnName(relation.targetTableName);

    const jsonArrayAgg = getJsonArrayAggFunc(dbType);
    const emptyArray = getEmptyJsonArray(dbType);

    const joinLeft = castToText(`j.${quoteIdentifier(junctionTargetCol, dbType)}`, dbType);
    const joinRight = castToText(`${nextAlias}.id`, dbType);
    const whereLeft = castToText(`j.${quoteIdentifier(junctionSourceCol, dbType)}`, dbType);
    const whereRight = castToText(`${parentRef}.id`, dbType);

    return `(select ${jsonArrayAgg}(${jsonObject}), ${emptyArray}) from ${quoteIdentifier(junctionTable, dbType)} j join ${quoteIdentifier(relation.targetTableName, dbType)} ${nextAlias} on ${joinLeft} = ${joinRight} where ${whereLeft} = ${whereRight})`;
  }

  return null;
}

export async function buildCTEStrategy(
  parentTable: string,
  parentMeta: TableMetadata,
  relationName: string,
  nestedFields: string[],
  dbType: DatabaseType,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
  limitedCTEName: string,
  parentIdColumn: string = 'id',
): Promise<string | null> {
  const relation = parentMeta.relations?.find(r => r.propertyName === relationName);
  if (!relation) {
    return null;
  }

  if (relation.type !== 'one-to-many' && relation.type !== 'many-to-many') {
    return null;
  }

  const targetMeta = await metadataGetter(relation.targetTableName);
  if (!targetMeta) {
    return null;
  }

  const rootFields: string[] = [];
  const subRelations = new Map<string, string[]>();

  for (const field of nestedFields) {
    if (field === '*' || !field.includes('.')) {
      rootFields.push(field);
    } else {
      const parts = field.split('.');
      const subRelName = parts[0];
      const remaining = parts.slice(1).join('.');

      if (!subRelations.has(subRelName)) {
        subRelations.set(subRelName, []);
      }
      subRelations.get(subRelName)!.push(remaining);
    }
  }

  const columns: string[] = [];

  if (rootFields.includes('*')) {
    const fkColumnsToOmit = new Set<string>();
    for (const rel of targetMeta.relations || []) {
      if (rel.type === 'many-to-one' || (rel.type === 'one-to-one' && !(rel as any).isInverse)) {
        const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.targetTableName);
        if (fkCol) fkColumnsToOmit.add(fkCol);
      }
    }
    for (const col of targetMeta.columns) {
      if (fkColumnsToOmit.has(col.name)) continue;
      columns.push(`'${col.name}', r.${quoteIdentifier(col.name, dbType)}`);
    }

    for (const rel of targetMeta.relations || []) {
      if (!subRelations.has(rel.propertyName)) {
        subRelations.set(rel.propertyName, ['id']);
      }
    }
  } else {
    for (const field of rootFields) {
      if (!field.includes('.')) {
        const col = targetMeta.columns.find(c => c.name === field);
        if (col) {
          columns.push(`'${col.name}', r.${quoteIdentifier(col.name, dbType)}`);
        }
      }
    }
  }

  for (const [subRelName, subFields] of subRelations.entries()) {
    const nestedSubquery = await buildNestedSubquery(
      relation.targetTableName,
      targetMeta,
      subRelName,
      subFields,
      dbType,
      metadataGetter,
      sortOptions.filter(s => s.field.startsWith(`${relationName}.${subRelName}`)),
      1,
      'r',
    );

    if (nestedSubquery) {
      columns.push(`'${subRelName}', ${nestedSubquery}`);
    }
  }

  if (columns.length === 0) {
    return null;
  }

  const jsonObjectFunc = getJsonObjectFunc(dbType);
  const jsonObject = `${jsonObjectFunc}(${columns.join(',')})`;
  const jsonArrayAgg = dbType === 'postgres' ? 'jsonb_agg' : getJsonArrayAggFunc(dbType);
  const emptyArray = dbType === 'postgres' ? "'[]'::jsonb" : getEmptyJsonArray(dbType);

  const quotedParentTable = quoteIdentifier(parentTable, dbType);
  const quotedTargetTable = quoteIdentifier(relation.targetTableName, dbType);
  const quotedParentId = quoteIdentifier(parentIdColumn, dbType);
  const quotedRelationName = quoteIdentifier(relationName, dbType);

  if (relation.type === 'one-to-many') {
    let fkColumn = relation.foreignKeyColumn;
    if (!fkColumn) {
      if (relation.inversePropertyName) {
        fkColumn = getForeignKeyColumnName(relation.inversePropertyName);
      } else {
        const inverseRelation = targetMeta.relations?.find(
          r => r.type === 'many-to-one' && r.targetTableName === parentTable
        );
        if (inverseRelation?.foreignKeyColumn) {
          fkColumn = inverseRelation.foreignKeyColumn;
        } else {
          return null;
        }
      }
    }

    const quotedFkCol = quoteIdentifier(fkColumn, dbType);
    const cteName = `${relationName}_agg`;

    return `${cteName} AS (
      SELECT 
        r.${quotedFkCol} as parent_id,
        ${jsonArrayAgg}(${jsonObject}) as ${quotedRelationName}
      FROM ${quotedTargetTable} r
      INNER JOIN ${limitedCTEName} l ON r.${quotedFkCol} = l.${quotedParentId}
      GROUP BY r.${quotedFkCol}
    )`;
  } else if (relation.type === 'many-to-many') {
    const junctionTable = relation.junctionTableName;
    if (!junctionTable) {
      return null;
    }

    const junctionSourceCol = relation.junctionSourceColumn || getForeignKeyColumnName(parentTable);
    const junctionTargetCol = relation.junctionTargetColumn || getForeignKeyColumnName(relation.targetTableName);

    const quotedJunctionTable = quoteIdentifier(junctionTable, dbType);
    const quotedJunctionSourceCol = quoteIdentifier(junctionSourceCol, dbType);
    const quotedJunctionTargetCol = quoteIdentifier(junctionTargetCol, dbType);
    const cteName = `${relationName}_agg`;

    return `${cteName} AS (
      SELECT 
        j.${quotedJunctionSourceCol} as parent_id,
        ${jsonArrayAgg}(${jsonObject}) as ${quotedRelationName}
      FROM ${quotedJunctionTable} j
      INNER JOIN ${quotedTargetTable} r ON j.${quotedJunctionTargetCol} = r.id
      INNER JOIN ${limitedCTEName} l ON j.${quotedJunctionSourceCol} = l.${quotedParentId}
      GROUP BY j.${quotedJunctionSourceCol}
    )`;
  }

  return null;
}
