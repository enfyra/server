import { DatabaseType } from '../../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../knex/utils/sql-schema-naming.util';
import { getPrimaryKeyColumn } from '../../../knex/utils/metadata-loader';
import { TableMetadata } from '../../../knex/types/knex-types';
import {
  quoteIdentifier,
  getJsonObjectFunc,
  getJsonArrayAggFunc,
  getEmptyJsonArray,
} from '../../../knex/utils/migration/sql-dialect';
import { logToFile } from '../../../../shared/utils/winston-logger';

export async function buildNestedSubquery(
  parentTable: string,
  parentMeta: TableMetadata,
  relationName: string,
  nestedFields: string[],
  dbType: DatabaseType,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  nestingLevel: number = 0,
  parentAliasOverride?: string,
  junctionAlias?: string,
  maxDepth?: number,
): Promise<string | null> {
  if (maxDepth !== undefined && nestingLevel >= maxDepth) {
    return null;
  }

  const relation = parentMeta.relations?.find(
    (r) => r.propertyName === relationName,
  );
  if (!relation) {
    logToFile(
      'debug',
      `Relation ${relationName} not found in ${parentTable}`,
      'NestedSubquery',
    );
    return null;
  }

  const targetTableName =
    (relation as any).targetTableName || relation.targetTable;
  const targetMeta = await metadataGetter(targetTableName);
  if (!targetMeta) {
    logToFile(
      'debug',
      `Target metadata not found for ${targetTableName}`,
      'NestedSubquery',
    );
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
  const parentAlias =
    parentAliasOverride || (nestingLevel <= 1 ? 'c' : `c${nestingLevel - 1}`);

  const columns: string[] = [];

  if (rootFields.includes('*')) {
    const fkColumnsToOmit = new Set<string>();
    for (const rel of targetMeta.relations || []) {
      if (
        rel.type === 'many-to-one' ||
        (rel.type === 'one-to-one' && !(rel as any).isInverse)
      ) {
        const relTargetTable = (rel as any).targetTableName || rel.targetTable;
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(relTargetTable);
        if (fkCol) fkColumnsToOmit.add(fkCol);
      }
    }
    for (const col of targetMeta.columns) {
      if (fkColumnsToOmit.has(col.name)) continue;
      columns.push(
        `'${col.name}', ${currentAlias}.${quoteIdentifier(col.name, dbType)}`,
      );
    }

    for (const rel of targetMeta.relations || []) {
      if (!subRelations.has(rel.propertyName)) {
        subRelations.set(rel.propertyName, ['id']);
      }
    }
  } else {
    for (const field of rootFields) {
      if (!field.includes('.')) {
        const col = targetMeta.columns.find((c) => c.name === field);
        if (col) {
          columns.push(
            `'${col.name}', ${currentAlias}.${quoteIdentifier(col.name, dbType)}`,
          );
        }
      }
    }
  }

  for (const [subRelName, subFields] of subRelations.entries()) {
    const nestedSubquery = await buildNestedSubquery(
      targetTableName,
      targetMeta,
      subRelName,
      subFields,
      dbType,
      metadataGetter,
      nestingLevel + 1,
      currentAlias,
      `j_${subRelName.replace(/[^a-zA-Z0-9]/g, '_')}_${nestingLevel + 1}`,
      maxDepth,
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

  const nextAlias = nestingLevel === 0 ? 'c' : `c${nestingLevel}`;

  const parentRef =
    nestingLevel === 0 ? quoteIdentifier(parentTable, dbType) : parentAlias;
  const targetPkColumn = getPrimaryKeyColumn(targetMeta);
  const targetPkName = targetPkColumn?.name || 'id';
  const parentPkColumn = getPrimaryKeyColumn(parentMeta);
  const parentPkName = parentPkColumn?.name || 'id';

  const buildNarrowSelect = (alias: string, extraCols: string[] = []): string => {
    const needed = new Set<string>();
    needed.add(targetPkName);
    for (const col of targetMeta.columns) {
      if (rootFields.includes('*') || rootFields.includes(col.name)) {
        needed.add(col.name);
      }
    }
    // Include FK columns needed by nested sub-relations (M2O/O2O WHERE clauses)
    for (const rel of targetMeta.relations || []) {
      if (
        subRelations.has(rel.propertyName) &&
        (rel.type === 'many-to-one' ||
          (rel.type === 'one-to-one' && !(rel as any).isInverse))
      ) {
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
        needed.add(fkCol);
      }
    }
    for (const c of extraCols) needed.add(c);
    return [...needed]
      .map((c) => `${alias}.${quoteIdentifier(c, dbType)}`)
      .join(', ');
  };

  if (
    relation.type === 'many-to-one' ||
    (relation.type === 'one-to-one' && !(relation as any).isInverse)
  ) {
    const fkColumn =
      relation.foreignKeyColumn || getForeignKeyColumnName(relationName);
    const leftSide = `${nextAlias}.${quoteIdentifier(targetPkName, dbType)}`;
    const rightSide = `${parentRef}.${quoteIdentifier(fkColumn, dbType)}`;
    return `(select ${jsonObject} from ${quoteIdentifier(targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide} limit 1)`;
  } else if (relation.type === 'one-to-one' && (relation as any).isInverse) {
    const fkColumn =
      relation.foreignKeyColumn ||
      getForeignKeyColumnName(relation.mappedBy || relationName);
    const leftSide = `${nextAlias}.${quoteIdentifier(fkColumn, dbType)}`;
    const rightSide = `${parentRef}.${quoteIdentifier(parentPkName, dbType)}`;
    return `(select ${jsonObject} from ${quoteIdentifier(targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide} limit 1)`;
  } else if (relation.type === 'one-to-many') {
    let fkColumn = relation.foreignKeyColumn;

    if (!fkColumn) {
      if (relation.mappedBy) {
        fkColumn = getForeignKeyColumnName(relation.mappedBy);
      } else {
        const inverseRelation = targetMeta.relations?.find(
          (r) =>
            r.type === 'many-to-one' &&
            ((r as any).targetTableName || r.targetTable) === parentTable,
        );
        if (inverseRelation?.foreignKeyColumn) {
          fkColumn = inverseRelation.foreignKeyColumn;
        } else {
          logToFile(
            'debug',
            `O2M relation ${relationName} missing foreignKeyColumn and mappedBy`,
            'NestedSubquery',
          );
          return null;
        }
      }
    }

    const jsonArrayAgg = getJsonArrayAggFunc(dbType);
    const emptyArray = getEmptyJsonArray(dbType);
    const leftSide = `${nextAlias}.${quoteIdentifier(fkColumn, dbType)}`;
    const rightSide = `${parentRef}.${quoteIdentifier(parentPkName, dbType)}`;
    if (dbType === 'mysql') {
      const narrowCols = buildNarrowSelect(nextAlias, [fkColumn]);
      return `(select ${jsonArrayAgg}(${jsonObject}), ${emptyArray}) from (select ${narrowCols} from ${quoteIdentifier(targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide} ORDER BY ${nextAlias}.${quoteIdentifier(targetPkName, dbType)} ASC) ${nextAlias})`;
    }
    const orderByInAgg = ` ORDER BY ${nextAlias}.${quoteIdentifier(targetPkName, dbType)} ASC`;
    return `(select ${jsonArrayAgg}(${jsonObject}${orderByInAgg}), ${emptyArray}) from ${quoteIdentifier(targetTableName, dbType)} ${nextAlias} where ${leftSide} = ${rightSide})`;
  } else if (relation.type === 'many-to-many') {
    const junctionTable = relation.junctionTableName;
    if (!junctionTable) {
      logToFile(
        'debug',
        `M2M relation ${relationName} missing junctionTableName`,
        'NestedSubquery',
      );
      return null;
    }

    const junctionSourceCol =
      relation.junctionSourceColumn || getForeignKeyColumnName(parentTable);
    const junctionTargetCol =
      relation.junctionTargetColumn || getForeignKeyColumnName(targetTableName);

    const jsonArrayAgg = getJsonArrayAggFunc(dbType);
    const emptyArray = getEmptyJsonArray(dbType);

    const jAlias =
      junctionAlias ||
      (nestingLevel === 0
        ? 'j'
        : `j_${relationName.replace(/[^a-zA-Z0-9]/g, '_')}_${nestingLevel}`);
    const joinLeft = `${jAlias}.${quoteIdentifier(junctionTargetCol, dbType)}`;
    const joinRight = `${nextAlias}.${quoteIdentifier(targetPkName, dbType)}`;
    const whereLeft = `${jAlias}.${quoteIdentifier(junctionSourceCol, dbType)}`;
    const whereRight = `${parentRef}.${quoteIdentifier(parentPkName, dbType)}`;

    if (dbType === 'mysql') {
      const narrowCols = buildNarrowSelect(nextAlias);
      return `(select ${jsonArrayAgg}(${jsonObject}), ${emptyArray}) from (select ${narrowCols} from ${quoteIdentifier(junctionTable, dbType)} ${jAlias} join ${quoteIdentifier(targetTableName, dbType)} ${nextAlias} on ${joinLeft} = ${joinRight} where ${whereLeft} = ${whereRight} ORDER BY ${nextAlias}.${quoteIdentifier(targetPkName, dbType)} ASC) ${nextAlias})`;
    }
    const orderByInAgg = ` ORDER BY ${nextAlias}.${quoteIdentifier(targetPkName, dbType)} ASC`;
    return `(select ${jsonArrayAgg}(${jsonObject}${orderByInAgg}), ${emptyArray}) from ${quoteIdentifier(junctionTable, dbType)} ${jAlias} join ${quoteIdentifier(targetTableName, dbType)} ${nextAlias} on ${joinLeft} = ${joinRight} where ${whereLeft} = ${whereRight})`;
  }

  return null;
}

export async function buildOwnerCTEStrategy(
  parentTable: string,
  parentMeta: TableMetadata,
  relationName: string,
  nestedFields: string[],
  dbType: DatabaseType,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  limitedCTEName: string,
  parentIdColumn: string = 'id',
  maxDepth?: number,
): Promise<string | null> {
  const relation = parentMeta.relations?.find(
    (r) => r.propertyName === relationName,
  );
  if (!relation) {
    return null;
  }

  if (
    relation.type !== 'many-to-one' &&
    !(relation.type === 'one-to-one' && !(relation as any).isInverse)
  ) {
    return null;
  }

  const targetTableName =
    (relation as any).targetTableName || relation.targetTable;
  const targetMeta = await metadataGetter(targetTableName);
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
      if (
        rel.type === 'many-to-one' ||
        (rel.type === 'one-to-one' && !(rel as any).isInverse)
      ) {
        const relTargetTable = (rel as any).targetTableName || rel.targetTable;
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(relTargetTable);
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
        const col = targetMeta.columns.find((c) => c.name === field);
        if (col) {
          columns.push(`'${col.name}', r.${quoteIdentifier(col.name, dbType)}`);
        }
      }
    }
  }

  for (const [subRelName, subFields] of subRelations.entries()) {
    const nestedSubquery = await buildNestedSubquery(
      targetTableName,
      targetMeta,
      subRelName,
      subFields,
      dbType,
      metadataGetter,
      1,
      'r',
      `j_${subRelName.replace(/[^a-zA-Z0-9]/g, '_')}_1`,
      maxDepth,
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

  const quotedParentTable = quoteIdentifier(parentTable, dbType);
  const quotedTargetTable = quoteIdentifier(targetTableName, dbType);
  const quotedLimitedCTE = quoteIdentifier(limitedCTEName, dbType);
  const quotedParentId = quoteIdentifier(parentIdColumn, dbType);
  const quotedParentIdCol = quoteIdentifier('parent_id', dbType);
  const quotedRelationName = quoteIdentifier(relationName, dbType);

  const targetPkColumn = getPrimaryKeyColumn(targetMeta);
  const targetPkName = targetPkColumn?.name || 'id';
  const quotedTargetPk = quoteIdentifier(targetPkName, dbType);

  const fkColumn =
    relation.foreignKeyColumn || getForeignKeyColumnName(relationName);
  const quotedFkCol = quoteIdentifier(fkColumn, dbType);

  return `${quoteIdentifier(`${relationName}_agg`, dbType)} AS (
    SELECT
      p.${quotedParentId} as ${quotedParentIdCol},
      (CASE
        WHEN p.${quotedFkCol} IS NULL THEN NULL
        ELSE ${jsonObject}
      END) as ${quotedRelationName}
    FROM ${quotedParentTable} p
    INNER JOIN ${quotedLimitedCTE} l ON p.${quotedParentId} = l.${quotedParentId}
    LEFT JOIN ${quotedTargetTable} r ON p.${quotedFkCol} = r.${quotedTargetPk}
  )`;
}

export async function buildCTEStrategy(
  parentTable: string,
  parentMeta: TableMetadata,
  relationName: string,
  nestedFields: string[],
  dbType: DatabaseType,
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  limitedCTEName: string,
  parentIdColumn: string = 'id',
  maxDepth?: number,
): Promise<string | null> {
  if (dbType === 'mysql') {
    return null;
  }

  const relation = parentMeta.relations?.find(
    (r) => r.propertyName === relationName,
  );
  if (!relation) {
    return null;
  }

  if (relation.type !== 'one-to-many' && relation.type !== 'many-to-many') {
    return null;
  }

  const targetTableName =
    (relation as any).targetTableName || relation.targetTable;
  const targetMeta = await metadataGetter(targetTableName);
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
      if (
        rel.type === 'many-to-one' ||
        (rel.type === 'one-to-one' && !(rel as any).isInverse)
      ) {
        const relTargetTable = (rel as any).targetTableName || rel.targetTable;
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(relTargetTable);
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
        const col = targetMeta.columns.find((c) => c.name === field);
        if (col) {
          columns.push(`'${col.name}', r.${quoteIdentifier(col.name, dbType)}`);
        }
      }
    }
  }

  for (const [subRelName, subFields] of subRelations.entries()) {
    const nestedSubquery = await buildNestedSubquery(
      targetTableName,
      targetMeta,
      subRelName,
      subFields,
      dbType,
      metadataGetter,
      1,
      'r',
      `j_${subRelName.replace(/[^a-zA-Z0-9]/g, '_')}_1`,
      maxDepth,
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
  const jsonArrayAggFunc = getJsonArrayAggFunc(dbType);
  const jsonArrayAgg =
    dbType === 'postgres'
      ? jsonArrayAggFunc.replace('json_agg', 'jsonb_agg')
      : jsonArrayAggFunc;
  const emptyArray =
    dbType === 'postgres' ? "'[]'::jsonb" : getEmptyJsonArray(dbType);

  const quotedParentTable = quoteIdentifier(parentTable, dbType);
  const quotedTargetTable = quoteIdentifier(targetTableName, dbType);
  const quotedParentId = quoteIdentifier(parentIdColumn, dbType);
  const quotedRelationName = quoteIdentifier(relationName, dbType);
  const targetPkColumn = targetMeta
    ? getPrimaryKeyColumn(targetMeta as any)
    : null;
  const targetPkName = targetPkColumn?.name || 'id';
  const parentPkColumn = parentMeta
    ? getPrimaryKeyColumn(parentMeta as any)
    : null;
  const parentPkName = parentPkColumn?.name || 'id';

  if (relation.type === 'one-to-many') {
    let fkColumn = relation.foreignKeyColumn;
    if (!fkColumn) {
      if (relation.mappedBy) {
        fkColumn = getForeignKeyColumnName(relation.mappedBy);
      } else {
        const inverseRelation = targetMeta.relations?.find(
          (r) =>
            r.type === 'many-to-one' &&
            ((r as any).targetTableName || r.targetTable) === parentTable,
        );
        if (inverseRelation?.foreignKeyColumn) {
          fkColumn = inverseRelation.foreignKeyColumn;
        } else {
          return null;
        }
      }
    }

    const quotedFkCol = quoteIdentifier(fkColumn, dbType);
    const quotedTargetPk = quoteIdentifier(targetPkName, dbType);
    const cteName = `${relationName}_agg`;
    const quotedCTEName = quoteIdentifier(cteName, dbType);
    const quotedLimitedCTE = quoteIdentifier(limitedCTEName, dbType);
    const quotedParentIdCol = quoteIdentifier('parent_id', dbType);

    return `${quotedCTEName} AS (
      SELECT
        r.${quotedFkCol} as ${quotedParentIdCol},
        ${jsonArrayAgg}(${jsonObject} ORDER BY r.${quotedTargetPk} ASC), ${emptyArray}) as ${quotedRelationName}
      FROM ${quotedTargetTable} r
      INNER JOIN ${quotedLimitedCTE} l ON r.${quotedFkCol} = l.${quotedParentId}
      GROUP BY r.${quotedFkCol}
    )`;
  } else if (relation.type === 'many-to-many') {
    const junctionTable = relation.junctionTableName;
    if (!junctionTable) {
      return null;
    }

    const junctionSourceCol =
      relation.junctionSourceColumn || getForeignKeyColumnName(parentTable);
    const junctionTargetCol =
      relation.junctionTargetColumn || getForeignKeyColumnName(targetTableName);

    const quotedJunctionTable = quoteIdentifier(junctionTable, dbType);
    const quotedJunctionSourceCol = quoteIdentifier(junctionSourceCol, dbType);
    const quotedJunctionTargetCol = quoteIdentifier(junctionTargetCol, dbType);
    const cteName = `${relationName}_agg`;
    const quotedCTEName = quoteIdentifier(cteName, dbType);
    const quotedLimitedCTE = quoteIdentifier(limitedCTEName, dbType);
    const quotedParentIdCol = quoteIdentifier('parent_id', dbType);
    const quotedTargetPk = quoteIdentifier(targetPkName, dbType);

    return `${quotedCTEName} AS (
      SELECT
        j.${quotedJunctionSourceCol} as ${quotedParentIdCol},
        ${jsonArrayAgg}(${jsonObject} ORDER BY r.${quotedTargetPk} ASC), ${emptyArray}) as ${quotedRelationName}
      FROM ${quotedJunctionTable} j
      INNER JOIN ${quotedTargetTable} r ON j.${quotedJunctionTargetCol} = r.${quotedTargetPk}
      INNER JOIN ${quotedLimitedCTE} l ON j.${quotedJunctionSourceCol} = l.${quotedParentId}
      GROUP BY j.${quotedJunctionSourceCol}
    )`;
  }

  return null;
}
