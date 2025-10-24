import { Knex } from 'knex';
import { TableMetadata } from '../../../knex/types/knex-types';
import { buildWhereClause } from './build-where-clause';
import { quoteIdentifier } from '../../../knex/utils/migration/sql-dialect';
import { separateFilters } from '../shared/filter-separator.util';

// Re-export for backwards compatibility
export { separateFilters };

export async function buildRelationSubquery(
  knex: Knex,
  tableName: string,
  relationName: string,
  relationFilter: any,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<string> {
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

  let subquery: Knex.QueryBuilder;

  switch (relation.type) {
    case 'one-to-many':
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(`${quoteIdentifier(targetTable, dbType)}.${quoteIdentifier(relation.foreignKeyColumn!, dbType)} = ${quoteIdentifier(tableName, dbType)}.${quoteIdentifier('id', dbType)}`);
      break;

    case 'many-to-many':
      subquery = knex(relation.junctionTableName!)
        .select(knex.raw('1'))
        .join(
          targetTable,
          `${quoteIdentifier(relation.junctionTableName!, dbType)}.${quoteIdentifier(relation.junctionTargetColumn!, dbType)}`,
          `${quoteIdentifier(targetTable, dbType)}.${quoteIdentifier('id', dbType)}`
        )
        .whereRaw(`${quoteIdentifier(relation.junctionTableName!, dbType)}.${quoteIdentifier(relation.junctionSourceColumn!, dbType)} = ${quoteIdentifier(tableName, dbType)}.${quoteIdentifier('id', dbType)}`);
      break;

    case 'many-to-one':
    case 'one-to-one':
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(`${quoteIdentifier(targetTable, dbType)}.${quoteIdentifier('id', dbType)} = ${quoteIdentifier(tableName, dbType)}.${quoteIdentifier(relation.foreignKeyColumn!, dbType)}`);
      break;

    default:
      throw new Error(`Unsupported relation type: ${relation.type}`);
  }

  if (getMetadata) {
    const targetMetadata = await getMetadata(targetTable);

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
      subquery = buildWhereClause(subquery, relationFilter, targetTable, dbType);
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
          buildWhereClause(this, fieldFilters, tableName, dbType);
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
        query.whereRaw(`EXISTS (${nestedSubquerySql})`);
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
        subqueries.push(`EXISTS (${nestedSubquerySql})`);
      }

      orParts.push({ fieldFilters, subqueries });
    }

    query.where(function() {
      for (let i = 0; i < orParts.length; i++) {
        const part = orParts[i];

        if (i === 0) {
          this.where(function() {
            if (Object.keys(part.fieldFilters).length > 0) {
              buildWhereClause(this, part.fieldFilters, tableName, dbType);
            }
            for (const subquerySql of part.subqueries) {
              this.whereRaw(subquerySql);
            }
          });
        } else {
          this.orWhere(function() {
            if (Object.keys(part.fieldFilters).length > 0) {
              buildWhereClause(this, part.fieldFilters, tableName, dbType);
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
        buildWhereClause(this, fieldFilters, tableName, dbType);
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
      query.whereRaw(`NOT EXISTS (${nestedSubquerySql})`);
    }
    return;
  }

  const { fieldFilters, relationFilters } = separateFilters(filter, metadata);

  if (Object.keys(fieldFilters).length > 0) {
    buildWhereClause(query, fieldFilters, tableName, dbType);
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
    query.whereRaw(`EXISTS (${nestedSubquerySql})`);
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
