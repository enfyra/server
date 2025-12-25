import { Knex } from 'knex';
import { TableMetadata } from '../../../knex/types/knex-types';
import { buildWhereClause } from './build-where-clause';
import { quoteIdentifier } from '../../../knex/utils/migration/sql-dialect';
import { separateFilters } from '../shared/filter-separator.util';

// Re-export for backwards compatibility
export { separateFilters };

function isUUIDColumn(columnName: string, metadata: TableMetadata | null | undefined): boolean {
  if (!metadata) return false;
  const column = metadata.columns.find(c => c.name === columnName);
  if (!column) return false;
  const type = column.type?.toLowerCase() || '';
  return type === 'uuid' || type === 'uuidv4' || type.includes('uuid');
}

function normalizeBoolean(value: any): boolean | null {
  if (value === true || value === 'true') return true;
  if (value === false || value === 'false') return false;
  return null;
}

function buildJoinCondition(
  leftTable: string,
  leftColumn: string,
  rightTable: string,
  rightColumn: string,
  leftMetadata: TableMetadata | null | undefined,
  rightMetadata: TableMetadata | null | undefined,
  dbType: string,
): string {
  const leftIsUUID = isUUIDColumn(leftColumn, leftMetadata);
  const rightIsUUID = isUUIDColumn(rightColumn, rightMetadata);
  
  const leftField = `${quoteIdentifier(leftTable, dbType)}.${quoteIdentifier(leftColumn, dbType)}`;
  const rightField = `${quoteIdentifier(rightTable, dbType)}.${quoteIdentifier(rightColumn, dbType)}`;
  
  if (dbType === 'postgres') {
    if (leftIsUUID && !rightIsUUID) {
      return `${leftField} = ${rightField}::uuid`;
    } else if (!leftIsUUID && rightIsUUID) {
      return `${leftField}::uuid = ${rightField}`;
    }
  }
  
  return `${leftField} = ${rightField}`;
}

export async function buildRelationSubquery(
  knex: Knex,
  tableName: string,
  relationName: string,
  relationFilter: any,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<string | null> {
  const relation = metadata.relations.find(r => r.propertyName === relationName);

  if (!relation) {
    throw new Error(`Relation "${relationName}" not found in table "${tableName}"`);
  }

  const targetTable = (relation as any).targetTableName || relation.targetTable;

  if (!targetTable) {
    throw new Error(
      `Relation "${relationName}" in table "${tableName}" is missing targetTable/targetTableName. ` +
      `Relation metadata: ${JSON.stringify(relation, null, 2)}`
    );
  }

  if ((relation.type === 'many-to-one' || relation.type === 'one-to-one') && relation.foreignKeyColumn) {
    const filterKeys = Object.keys(relationFilter || {});
    const isOnlyIdNull = filterKeys.length === 1 &&
                         filterKeys[0] === 'id' &&
                         typeof relationFilter.id === 'object' &&
                         ('_is_null' in relationFilter.id || '_is_not_null' in relationFilter.id);

    if (isOnlyIdNull) {
      return null;
    }
  }

  const targetMetadata = getMetadata ? await getMetadata(targetTable) : null;

  let subquery: Knex.QueryBuilder;

  switch (relation.type) {
    case 'one-to-many':
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(buildJoinCondition(
          targetTable,
          relation.foreignKeyColumn!,
          tableName,
          'id',
          targetMetadata,
          metadata,
          dbType,
        ));
      break;

    case 'many-to-many':
      const junctionMetadata = getMetadata ? await getMetadata(relation.junctionTableName!) : null;
      subquery = knex(relation.junctionTableName!)
        .select(knex.raw('1'))
        .join(
          targetTable,
          knex.raw(buildJoinCondition(
            relation.junctionTableName!,
            relation.junctionTargetColumn!,
            targetTable,
            'id',
            junctionMetadata,
            targetMetadata,
            dbType,
          )),
        )
        .whereRaw(buildJoinCondition(
          relation.junctionTableName!,
          relation.junctionSourceColumn!,
          tableName,
          'id',
          junctionMetadata,
          metadata,
          dbType,
        ));
      break;

    case 'many-to-one':
    case 'one-to-one':
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(buildJoinCondition(
          targetTable,
          'id',
          tableName,
          relation.foreignKeyColumn!,
          targetMetadata,
          metadata,
          dbType,
        ));
      break;

    default:
      throw new Error(`Unsupported relation type: ${relation.type}`);
  }

  if (getMetadata) {
    if (targetMetadata) {
      await applyFiltersToSubquery(
        knex,
        subquery,
        relationFilter,
        targetTable,
        targetMetadata,
        dbType,
        getMetadata,
      );
    } else {
      subquery = buildWhereClause(subquery, relationFilter, targetTable, dbType, targetMetadata || undefined);
    }
  } else {
    subquery = buildWhereClause(subquery, relationFilter, targetTable, dbType);
  }

  return subquery.toString();
}

async function applyFiltersToSubquery(
  knex: Knex,
  query: Knex.QueryBuilder,
  filter: any,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<void> {
  if (!filter || typeof filter !== 'object') {
    return;
  }

  if (filter._and && Array.isArray(filter._and)) {
    for (const condition of filter._and) {
      const { fieldFilters, relationFilters } = separateFilters(condition, metadata);

      query.where(function() {
        if (Object.keys(fieldFilters).length > 0) {
          buildWhereClause(this, fieldFilters, tableName, dbType, metadata);
        }
      });

      for (const [nestedRelName, nestedRelFilter] of Object.entries(relationFilters)) {
        const nestedSubquerySql = await buildRelationSubquery(
          knex,
          tableName,
          nestedRelName,
          nestedRelFilter,
          metadata,
          dbType,
          getMetadata,
        );

        if (nestedSubquerySql === null) {
          const relation = metadata.relations.find(r => r.propertyName === nestedRelName);
          if (relation && relation.foreignKeyColumn) {
            const fkColumn = `${tableName}.${relation.foreignKeyColumn}`;
            const filterObj = nestedRelFilter as any;
            const idFilter = filterObj.id;
            
            if (idFilter && typeof idFilter === 'object') {
              const isNullValue = normalizeBoolean(idFilter._is_null);
              const isNotNullValue = normalizeBoolean(idFilter._is_not_null);
              
              if (isNullValue === true) {
                query.whereNull(fkColumn);
              } else if (isNullValue === false) {
                query.whereNotNull(fkColumn);
              } else if (isNotNullValue === true) {
                query.whereNotNull(fkColumn);
              } else if (isNotNullValue === false) {
                query.whereNull(fkColumn);
              } else if (idFilter._eq !== undefined) {
                query.where(fkColumn, '=', idFilter._eq);
              } else if (idFilter._neq !== undefined) {
                query.where(fkColumn, '!=', idFilter._neq);
              } else if (idFilter._in !== undefined) {
                const inValues = Array.isArray(idFilter._in) ? idFilter._in : [idFilter._in];
                query.whereIn(fkColumn, inValues);
              } else if (idFilter._not_in !== undefined) {
                const notInValues = Array.isArray(idFilter._not_in) ? idFilter._not_in : [idFilter._not_in];
                query.whereNotIn(fkColumn, notInValues);
              }
            }
          }
        } else {
          query.whereRaw(`EXISTS (${nestedSubquerySql})`);
        }
      }
    }
    return;
  }

  if (filter._or && Array.isArray(filter._or)) {
    const orParts: Array<{fieldFilters: any, subqueries: string[]}> = [];

    for (const condition of filter._or) {
      const { fieldFilters, relationFilters } = separateFilters(condition, metadata);
      const subqueries: string[] = [];

      for (const [nestedRelName, nestedRelFilter] of Object.entries(relationFilters)) {
        const nestedSubquerySql = await buildRelationSubquery(
          knex,
          tableName,
          nestedRelName,
          nestedRelFilter,
          metadata,
          dbType,
          getMetadata,
        );

        if (nestedSubquerySql !== null) {
          subqueries.push(`EXISTS (${nestedSubquerySql})`);
        } else {
          const relation = metadata.relations.find(r => r.propertyName === nestedRelName);
          if (relation && relation.foreignKeyColumn) {
            const fkColumn = `${tableName}.${relation.foreignKeyColumn}`;
            const filterObj = nestedRelFilter as any;
            const idFilter = filterObj.id;
            
            if (idFilter && typeof idFilter === 'object') {
              const isNullValue = normalizeBoolean(idFilter._is_null);
              const isNotNullValue = normalizeBoolean(idFilter._is_not_null);
              
              if (isNullValue === true) {
                subqueries.push(`${fkColumn} IS NULL`);
              } else if (isNullValue === false) {
                subqueries.push(`${fkColumn} IS NOT NULL`);
              } else if (isNotNullValue === true) {
                subqueries.push(`${fkColumn} IS NOT NULL`);
              } else if (isNotNullValue === false) {
                subqueries.push(`${fkColumn} IS NULL`);
              } else if (idFilter._eq !== undefined) {
                subqueries.push(`${fkColumn} = ${typeof idFilter._eq === 'string' ? `'${idFilter._eq}'` : idFilter._eq}`);
              } else if (idFilter._neq !== undefined) {
                subqueries.push(`${fkColumn} != ${typeof idFilter._neq === 'string' ? `'${idFilter._neq}'` : idFilter._neq}`);
              } else if (idFilter._in !== undefined) {
                const inValues = Array.isArray(idFilter._in) ? idFilter._in : [idFilter._in];
                const inStr = inValues.map(v => typeof v === 'string' ? `'${v}'` : v).join(', ');
                subqueries.push(`${fkColumn} IN (${inStr})`);
              } else if (idFilter._not_in !== undefined) {
                const notInValues = Array.isArray(idFilter._not_in) ? idFilter._not_in : [idFilter._not_in];
                const notInStr = notInValues.map(v => typeof v === 'string' ? `'${v}'` : v).join(', ');
                subqueries.push(`${fkColumn} NOT IN (${notInStr})`);
              }
            }
          }
        }
      }

      orParts.push({ fieldFilters, subqueries });
    }

    query.where(function() {
      for (let i = 0; i < orParts.length; i++) {
        const part = orParts[i];

        if (i === 0) {
          this.where(function() {
            if (Object.keys(part.fieldFilters).length > 0) {
              buildWhereClause(this, part.fieldFilters, tableName, dbType, metadata);
            }
            for (const subquerySql of part.subqueries) {
              this.whereRaw(subquerySql);
            }
          });
        } else {
          this.orWhere(function() {
            if (Object.keys(part.fieldFilters).length > 0) {
              buildWhereClause(this, part.fieldFilters, tableName, dbType, metadata);
            }
            for (const subquerySql of part.subqueries) {
              this.whereRaw(subquerySql);
            }
          });
        }
      }
    });

    return;
  }

  if (filter._not) {
    const { fieldFilters, relationFilters } = separateFilters(filter._not, metadata);

    query.whereNot(function() {
      if (Object.keys(fieldFilters).length > 0) {
        buildWhereClause(this, fieldFilters, tableName, dbType, metadata);
      }
    });

    for (const [nestedRelName, nestedRelFilter] of Object.entries(relationFilters)) {
      const nestedSubquerySql = await buildRelationSubquery(
        knex,
        tableName,
        nestedRelName,
        nestedRelFilter,
        metadata,
        dbType,
        getMetadata,
      );

      if (nestedSubquerySql === null) {
        const relation = metadata.relations.find(r => r.propertyName === nestedRelName);
        if (relation && relation.foreignKeyColumn) {
          const fkColumn = `${tableName}.${relation.foreignKeyColumn}`;
          const filterObj = nestedRelFilter as any;
          const idFilter = filterObj.id;
          
          if (idFilter && typeof idFilter === 'object') {
            const isNullValue = normalizeBoolean(idFilter._is_null);
            const isNotNullValue = normalizeBoolean(idFilter._is_not_null);
            
            if (isNullValue === true) {
              query.whereNotNull(fkColumn);
            } else if (isNullValue === false) {
              query.whereNull(fkColumn);
            } else if (isNotNullValue === true) {
              query.whereNull(fkColumn);
            } else if (isNotNullValue === false) {
              query.whereNotNull(fkColumn);
            } else if (idFilter._eq !== undefined) {
              query.where(fkColumn, '!=', idFilter._eq);
            } else if (idFilter._neq !== undefined) {
              query.where(fkColumn, '=', idFilter._neq);
            } else if (idFilter._in !== undefined) {
              const inValues = Array.isArray(idFilter._in) ? idFilter._in : [idFilter._in];
              query.whereNotIn(fkColumn, inValues);
            } else if (idFilter._not_in !== undefined) {
              const notInValues = Array.isArray(idFilter._not_in) ? idFilter._not_in : [idFilter._not_in];
              query.whereIn(fkColumn, notInValues);
            }
          }
        }
      } else {
        query.whereRaw(`NOT EXISTS (${nestedSubquerySql})`);
      }
    }
    return;
  }

  const { fieldFilters, relationFilters } = separateFilters(filter, metadata);

  if (Object.keys(fieldFilters).length > 0) {
    buildWhereClause(query, fieldFilters, tableName, dbType, metadata);
  }

  for (const [nestedRelName, nestedRelFilter] of Object.entries(relationFilters)) {
    const nestedSubquerySql = await buildRelationSubquery(
      knex,
      tableName,
      nestedRelName,
      nestedRelFilter,
      metadata,
      dbType,
      getMetadata,
    );

    if (nestedSubquerySql === null) {
      const relation = metadata.relations.find(r => r.propertyName === nestedRelName);
      if (relation && relation.foreignKeyColumn) {
        const fkColumn = `${tableName}.${relation.foreignKeyColumn}`;
        const filterObj = nestedRelFilter as any;
        const idFilter = filterObj.id;
        
        if (idFilter && typeof idFilter === 'object') {
          const isNullValue = normalizeBoolean(idFilter._is_null);
          const isNotNullValue = normalizeBoolean(idFilter._is_not_null);
          
          if (isNullValue === true) {
            query.whereNull(fkColumn);
          } else if (isNullValue === false) {
            query.whereNotNull(fkColumn);
          } else if (isNotNullValue === true) {
            query.whereNotNull(fkColumn);
          } else if (isNotNullValue === false) {
            query.whereNull(fkColumn);
          } else if (idFilter._eq !== undefined) {
            query.where(fkColumn, '=', idFilter._eq);
          } else if (idFilter._neq !== undefined) {
            query.where(fkColumn, '!=', idFilter._neq);
          } else if (idFilter._in !== undefined) {
            const inValues = Array.isArray(idFilter._in) ? idFilter._in : [idFilter._in];
            query.whereIn(fkColumn, inValues);
          } else if (idFilter._not_in !== undefined) {
            const notInValues = Array.isArray(idFilter._not_in) ? idFilter._not_in : [idFilter._not_in];
            query.whereNotIn(fkColumn, notInValues);
          }
        }
      }
    } else {
      query.whereRaw(`EXISTS (${nestedSubquerySql})`);
    }
  }
}

export async function applyRelationFilters(
  knex: Knex,
  query: Knex.QueryBuilder,
  filter: any,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<void> {
  await applyFiltersToSubquery(knex, query, filter, tableName, metadata, dbType, getMetadata);
}
