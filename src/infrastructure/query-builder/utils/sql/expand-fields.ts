import { DatabaseType } from '../../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../knex/utils/sql-schema-naming.util';
import { getPrimaryKeyColumn } from '../../../knex/utils/metadata-loader';
import { quoteIdentifier } from '../../../knex/utils/migration/sql-dialect';
import { BatchFetchDescriptor } from './batch-relation-fetcher';

export interface LimitedCteSortJoinStep {
  targetTable: string;
  fkCol: string;
  pkCol: string;
}

export interface LimitedCteSortJoin {
  steps: LimitedCteSortJoinStep[];
  sortField: string;
  direction: 'asc' | 'desc';
}

interface FieldExpansionResult {
  select: string[];
  cteClauses?: string[];
  batchFetchDescriptors: BatchFetchDescriptor[];
}

interface TableMetadata {
  name: string;
  columns: Array<{ name: string; type: string }>;
  relations: Array<{
    propertyName: string;
    type: 'many-to-one' | 'one-to-many' | 'one-to-one' | 'many-to-many';
    targetTableName: string;
    foreignKeyColumn?: string;
    junctionTableName?: string;
    junctionSourceColumn?: string;
    junctionTargetColumn?: string;
  }>;
}

export async function expandFieldsToJoinsAndSelect(
  tableName: string,
  fields: string[],
  metadataGetter: (tableName: string) => Promise<TableMetadata | null>,
  dbType: DatabaseType,
  limit?: number,
  orderByClause?: string,
  whereClause?: string,
  offset?: number,
  limitedCteSortJoin?: LimitedCteSortJoin,
  maxDepth?: number,
): Promise<FieldExpansionResult> {
  const select: string[] = [];
  const batchFetchDescriptors: BatchFetchDescriptor[] = [];

  const baseMeta = await metadataGetter(tableName);
  if (!baseMeta) {
    return { select: [], batchFetchDescriptors: [] };
  }

  const fieldsByRelation = new Map<string, string[]>();

  for (const field of fields) {
    if (field === '*' || !field.includes('.')) {
      if (!fieldsByRelation.has('')) {
        fieldsByRelation.set('', []);
      }
      fieldsByRelation.get('')!.push(field);
    } else {
      const parts = field.split('.');
      const relationName = parts[0];
      const remainingPath = parts.slice(1).join('.');

      if (!fieldsByRelation.has(relationName)) {
        fieldsByRelation.set(relationName, []);
      }
      fieldsByRelation.get(relationName)!.push(remainingPath);
    }
  }

  const rootFields = fieldsByRelation.get('') || [];
  for (const field of rootFields) {
    if (field === '*') {
      const fkColumnsToOmit = new Set<string>();
      for (const rel of baseMeta.relations || []) {
        if (
          rel.type === 'many-to-one' ||
          (rel.type === 'one-to-one' && !(rel as any).isInverse)
        ) {
          const fkCol =
            rel.foreignKeyColumn ||
            getForeignKeyColumnName(rel.targetTableName);
          if (fkCol) fkColumnsToOmit.add(fkCol);
        }
      }
      const addedColumnNames = new Set<string>();
      for (const col of baseMeta.columns) {
        if (fkColumnsToOmit.has(col.name)) continue;
        select.push(`${tableName}.${col.name}`);
        addedColumnNames.add(col.name);
      }

      for (const rel of baseMeta.relations || []) {
        if (
          !fieldsByRelation.has(rel.propertyName) &&
          !addedColumnNames.has(rel.propertyName)
        ) {
          fieldsByRelation.set(rel.propertyName, ['id']);
        }
      }

      continue;
    }

    if (field.includes('.')) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingColumn = baseMeta.columns?.find((c) => c.name === field);
    if (matchingColumn) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingRelation = baseMeta.relations?.find(
      (r) => r.propertyName === field,
    );
    if (matchingRelation) {
      if (!fieldsByRelation.has(field)) {
        fieldsByRelation.set(field, ['id']);
      }
      continue;
    }
  }

  const cteClauses: string[] = [];
  const useCTE =
    (dbType === 'postgres' || dbType === 'mysql') &&
    limit !== undefined &&
    (!!orderByClause || !!limitedCteSortJoin);

  let limitedCTEName: string | null = null;
  if (useCTE) {
    limitedCTEName = `limited_${tableName}`;
    const quotedTable = quoteIdentifier(tableName, dbType);
    const wherePart = whereClause ? ` ${whereClause}` : '';
    let orderByInCTE = orderByClause || '';
    if (orderByInCTE) {
      const parts = orderByInCTE.split(' ').map((part) => {
        if (['ORDER', 'BY', 'ASC', 'DESC'].includes(part.toUpperCase())) {
          return part;
        }
        if (part.match(/^[a-zA-Z_][a-zA-Z0-9_]*$/)) {
          return quoteIdentifier(part, dbType);
        }
        return part;
      });
      orderByInCTE = parts.join(' ');
    }
    const limitSQL =
      limit !== undefined && limit !== null && limit > 0
        ? `LIMIT ${limit}`
        : '';
    const offsetSQL =
      offset !== undefined && offset !== null && offset > 0
        ? `OFFSET ${offset}`
        : '';
    const quotedLimitedCTE = quoteIdentifier(limitedCTEName, dbType);
    const pkColumn = baseMeta ? getPrimaryKeyColumn(baseMeta as any) : null;
    const pkName = pkColumn?.name || 'id';
    const quotedPkCol = quoteIdentifier(pkName, dbType);

    let sortJoinFragment = '';
    let effectiveOrderBy = orderByInCTE;

    if (limitedCteSortJoin) {
      let lastRef = quotedTable;
      for (let i = 0; i < limitedCteSortJoin.steps.length; i++) {
        const step = limitedCteSortJoin.steps[i];
        const stepAlias = i === 0 ? 's_sort' : `s_sort_${i}`;
        const qTarget = quoteIdentifier(step.targetTable, dbType);
        const qPk = quoteIdentifier(step.pkCol, dbType);
        const qFk = quoteIdentifier(step.fkCol, dbType);
        sortJoinFragment += ` LEFT JOIN ${qTarget} ${stepAlias} ON ${stepAlias}.${qPk} = ${lastRef}.${qFk}`;
        lastRef = stepAlias;
      }
      const finalAlias =
        limitedCteSortJoin.steps.length === 1
          ? 's_sort'
          : `s_sort_${limitedCteSortJoin.steps.length - 1}`;
      const qSortField = quoteIdentifier(limitedCteSortJoin.sortField, dbType);
      const sortOrderBy = `ORDER BY ${finalAlias}.${qSortField} ${limitedCteSortJoin.direction.toUpperCase()}`;
      if (orderByInCTE) {
        effectiveOrderBy = `${sortOrderBy}, ${orderByInCTE.replace(/^ORDER BY /i, '')}`;
      } else {
        effectiveOrderBy = sortOrderBy;
      }
    }

    const selectPk = sortJoinFragment
      ? `${quotedTable}.${quotedPkCol}`
      : quotedPkCol;
    cteClauses.push(`${quoteIdentifier(limitedCTEName, dbType)} AS (
      SELECT ${selectPk} FROM ${quotedTable}${sortJoinFragment}${wherePart}${effectiveOrderBy ? ' ' + effectiveOrderBy : ''}${limitSQL ? ' ' + limitSQL : ''}${offsetSQL ? ' ' + offsetSQL : ''}
    )`);
  }

  fieldsByRelation.forEach((nestedFields, relationName) => {
    if (relationName === '') return;

    const relation = baseMeta.relations?.find(
      (r) => r.propertyName === relationName,
    );
    if (!relation) return;

    const isOwnerRelation =
      relation.type === 'many-to-one' ||
      (relation.type === 'one-to-one' && !(relation as any).isInverse);

    if (isOwnerRelation) {
      const fkColumn =
        relation.foreignKeyColumn ||
        getForeignKeyColumnName(
          relation.targetTableName || (relation as any).targetTable,
        );
      const quotedTable = quoteIdentifier(tableName, dbType);
      const quotedFk = quoteIdentifier(fkColumn, dbType);
      const quotedAlias = quoteIdentifier(relationName, dbType);
      select.push(`${quotedTable}.${quotedFk} as ${quotedAlias}`);
    }

    const targetTable =
      relation.targetTableName || (relation as any).targetTable;

    batchFetchDescriptors.push({
      relationName,
      type: relation.type,
      targetTable,
      fields: nestedFields,
      isInverse: (relation as any).isInverse,
      fkColumn: relation.foreignKeyColumn,
      mappedBy: (relation as any).mappedBy,
      junctionTableName: relation.junctionTableName,
      junctionSourceColumn: relation.junctionSourceColumn,
      junctionTargetColumn: relation.junctionTargetColumn,
    });
  });

  return {
    select,
    cteClauses: cteClauses.length > 0 ? cteClauses : undefined,
    batchFetchDescriptors,
  };
}
