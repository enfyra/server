import { DatabaseType } from '../../../../shared/types/query-builder.types';
import { getForeignKeyColumnName } from '../../../knex/utils/naming-helpers';
import { getPrimaryKeyColumn } from '../../../knex/utils/metadata-loader';
import { buildNestedSubquery, buildCTEStrategy } from './nested-subquery-builder';
import { quoteIdentifier, getEmptyJsonArray } from '../../../knex/utils/migration/sql-dialect';

interface FieldExpansionResult {
  select: string[];
  cteClauses?: string[];
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
  sortOptions: Array<{ field: string; direction: 'asc' | 'desc' }> = [],
  listTables?: () => Promise<string[]>,
  limit?: number,
  orderByClause?: string,
  whereClause?: string,
  offset?: number,
): Promise<FieldExpansionResult> {
  function findSortForPath(pathPrefix: string): { field: string; direction: 'asc' | 'desc' } | null {
    const found = sortOptions.find(s => s.field.startsWith(pathPrefix + '.') || s.field === pathPrefix);
    if (!found) return null;
    return found;
  }

  const select: string[] = [];

  const baseMeta = await metadataGetter(tableName);
  if (!baseMeta) {
    return { select: [] };
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
        if (rel.type === 'many-to-one' || (rel.type === 'one-to-one' && !(rel as any).isInverse)) {
          const fkCol = rel.foreignKeyColumn || getForeignKeyColumnName(rel.targetTableName);
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
        if (!fieldsByRelation.has(rel.propertyName) && !addedColumnNames.has(rel.propertyName)) {
          fieldsByRelation.set(rel.propertyName, ['id']);
        }
      }

      continue;
    }

    if (field.includes('.')) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingColumn = baseMeta.columns?.find(c => c.name === field);
    if (matchingColumn) {
      select.push(`${tableName}.${field}`);
      continue;
    }

    const matchingRelation = baseMeta.relations?.find(r => r.propertyName === field);
    if (matchingRelation) {
      if (!fieldsByRelation.has(field)) {
        fieldsByRelation.set(field, ['id']);
      }
      continue;
    }
  }

  const cteClauses: string[] = [];
  const useCTE = (dbType === 'postgres' || dbType === 'mysql') && limit !== undefined && limit > 0 && orderByClause;

  let limitedCTEName: string | null = null;
  if (useCTE) {
    limitedCTEName = `limited_${tableName}`;
    const quotedTable = quoteIdentifier(tableName, dbType);
    const wherePart = whereClause ? ` ${whereClause}` : '';
    let orderByInCTE = orderByClause || '';
    if (orderByInCTE) {
      const parts = orderByInCTE.split(' ').map(part => {
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
    const limitSQL = limit ? `LIMIT ${limit}` : '';
    const offsetSQL = offset ? `OFFSET ${offset}` : '';
    const quotedLimitedCTE = quoteIdentifier(limitedCTEName, dbType);
    const pkColumn = baseMeta ? getPrimaryKeyColumn(baseMeta as any) : null;
    const pkName = pkColumn?.name || 'id';
    const quotedPkCol = quoteIdentifier(pkName, dbType);
    cteClauses.push(`${quotedLimitedCTE} AS (
      SELECT ${quotedPkCol} FROM ${quotedTable}${wherePart}${orderByInCTE ? ' ' + orderByInCTE : ''}${limitSQL ? ' ' + limitSQL : ''}${offsetSQL ? ' ' + offsetSQL : ''}
    )`);
  }

  for (const [relationName, nestedFields] of fieldsByRelation.entries()) {
    if (relationName === '') continue;

    const relation = baseMeta.relations?.find(r => r.propertyName === relationName);
    if (!relation) {
      continue;
    }

    const isIdOnly = nestedFields.length === 1 && (nestedFields[0] === 'id' || nestedFields[0] === '_id');
    const isOwnerRelation = relation.type === 'many-to-one' || (relation.type === 'one-to-one' && !(relation as any).isInverse);

    if (isIdOnly && isOwnerRelation) {
      const fkColumn = relation.foreignKeyColumn || getForeignKeyColumnName(relation.targetTableName);
      const jsonObjectFunc = dbType === 'postgres' ? 'jsonb_build_object' : 'JSON_OBJECT';
      const quotedTable = quoteIdentifier(tableName, dbType);
      const quotedFkCol = quoteIdentifier(fkColumn, dbType);
      const quotedRelation = quoteIdentifier(relationName, dbType);
      const fkRef = `${quotedTable}.${quotedFkCol}`;

      const mapping = `(CASE WHEN ${fkRef} IS NULL THEN NULL ELSE ${jsonObjectFunc}('id', ${fkRef}) END) as ${quotedRelation}`;
      select.push(mapping);
    } else if (useCTE && limitedCTEName && (relation.type === 'one-to-many' || relation.type === 'many-to-many')) {
      const cteClause = await buildCTEStrategy(
        tableName,
        baseMeta as any,
        relationName,
        nestedFields,
        dbType,
        metadataGetter as any,
        sortOptions,
        limitedCTEName,
      );

      if (cteClause) {
        cteClauses.push(cteClause);
        const cteName = `${relationName}_agg`;
        const quotedCTEName = quoteIdentifier(cteName, dbType);
        const quotedRelation = quoteIdentifier(relationName, dbType);
        const emptyArray = getEmptyJsonArray(dbType);
        const tableAlias = useCTE ? 't' : tableName;
        const quotedRelationInCTE = quoteIdentifier(relationName, dbType);
        select.push(`COALESCE(${quotedCTEName}.${quotedRelationInCTE}, ${emptyArray}) as ${quotedRelation}`);
      } else {
        const subquery = await buildNestedSubquery(
          tableName,
          baseMeta as any,
          relationName,
          nestedFields,
          dbType,
          metadataGetter as any,
          sortOptions,
        );

        if (subquery) {
          select.push(`${subquery} as ${quoteIdentifier(relationName, dbType)}`);
        }
      }
    } else {
      const subquery = await buildNestedSubquery(
        tableName,
        baseMeta as any,
        relationName,
        nestedFields,
        dbType,
        metadataGetter as any,
        sortOptions,
      );

      if (subquery) {
        select.push(`${subquery} as ${quoteIdentifier(relationName, dbType)}`);
      }
    }
  }

  return { select, cteClauses: cteClauses.length > 0 ? cteClauses : undefined };
}
