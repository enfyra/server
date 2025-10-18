import { Knex } from 'knex';
import { TableMetadata } from '../../../shared/utils/knex-types';
import { buildWhereClause } from './build-where-clause';

/**
 * Check if a filter contains any relations (recursively checks logical operators)
 */
function hasAnyRelations(filter: any, relationNames: Set<string>): boolean {
  if (!filter || typeof filter !== 'object') {
    return false;
  }

  for (const [key, value] of Object.entries(filter)) {
    if (key === '_and' || key === '_or') {
      if (Array.isArray(value)) {
        for (const condition of value) {
          if (hasAnyRelations(condition, relationNames)) {
            return true;
          }
        }
      }
    } else if (key === '_not') {
      if (hasAnyRelations(value, relationNames)) {
        return true;
      }
    } else if (relationNames.has(key)) {
      return true;
    }
  }

  return false;
}

/**
 * Separate field filters from relation filters
 *
 * @param filter - Filter object
 * @param metadata - Table metadata to identify relations
 * @returns Separated field and relation filters
 */
export function separateFilters(
  filter: any,
  metadata: TableMetadata,
): { fieldFilters: any; relationFilters: any; hasRelations: boolean } {
  if (!filter || typeof filter !== 'object') {
    return { fieldFilters: {}, relationFilters: {}, hasRelations: false };
  }

  const fieldFilters: any = {};
  const relationFilters: any = {};

  // Get relation names from metadata
  const relationNames = new Set(metadata.relations.map(r => r.propertyName));

  for (const [key, value] of Object.entries(filter)) {
    // Logical operators stay in field filters (will be processed separately)
    if (key === '_and' || key === '_or' || key === '_not') {
      fieldFilters[key] = value;
      continue;
    }

    // Check if key is a relation
    if (relationNames.has(key)) {
      relationFilters[key] = value;
    } else {
      fieldFilters[key] = value;
    }
  }

  // Check if there are any relations anywhere in the filter
  const hasRelations = hasAnyRelations(filter, relationNames);

  return { fieldFilters, relationFilters, hasRelations };
}

/**
 * Build WHERE EXISTS subquery for relation filter
 *
 * @param knex - Knex instance
 * @param tableName - Main table name
 * @param relationName - Relation property name
 * @param relationFilter - Filter for relation
 * @param metadata - Main table metadata
 * @param dbType - Database type
 * @param getMetadata - Function to get metadata for nested relations
 * @returns SQL string for EXISTS subquery
 */
export async function buildRelationSubquery(
  knex: Knex,
  tableName: string,
  relationName: string,
  relationFilter: any,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<string> {
  // Find relation in metadata
  const relation = metadata.relations.find(r => r.propertyName === relationName);

  if (!relation) {
    throw new Error(`Relation "${relationName}" not found in table "${tableName}"`);
  }

  // Get target table name (support both targetTable and targetTableName for backward compatibility)
  const targetTable = (relation as any).targetTableName || relation.targetTable;

  // Validate relation has targetTable
  if (!targetTable) {
    throw new Error(
      `Relation "${relationName}" in table "${tableName}" is missing targetTable/targetTableName. ` +
      `Relation metadata: ${JSON.stringify(relation, null, 2)}`
    );
  }

  let subquery: Knex.QueryBuilder;

  // Build subquery based on relation type
  switch (relation.type) {
    case 'one-to-many':
      // SELECT 1 FROM related_table WHERE related_table.fk = main_table.id
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(`${targetTable}.${relation.foreignKeyColumn} = ${tableName}.id`);
      break;

    case 'many-to-many':
      // SELECT 1 FROM junction JOIN target WHERE junction.source_fk = main.id
      subquery = knex(relation.junctionTableName!)
        .select(knex.raw('1'))
        .join(
          targetTable,
          `${relation.junctionTableName}.${relation.junctionTargetColumn}`,
          `${targetTable}.id`
        )
        .whereRaw(`${relation.junctionTableName}.${relation.junctionSourceColumn} = ${tableName}.id`);
      break;

    case 'many-to-one':
    case 'one-to-one':
      // SELECT 1 FROM target WHERE target.id = main.fk_column
      subquery = knex(targetTable)
        .select(knex.raw('1'))
        .whereRaw(`${targetTable}.id = ${tableName}.${relation.foreignKeyColumn}`);
      break;

    default:
      throw new Error(`Unsupported relation type: ${relation.type}`);
  }

  // Apply filters to subquery
  if (getMetadata) {
    const targetMetadata = await getMetadata(targetTable);

    if (targetMetadata) {
      // Apply combined filters recursively
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
      // No metadata, treat all as field filters
      subquery = buildWhereClause(subquery, relationFilter, targetTable, dbType);
    }
  } else {
    // No getMetadata callback, treat all as field filters
    subquery = buildWhereClause(subquery, relationFilter, targetTable, dbType);
  }

  return subquery.toString();
}

/**
 * Apply filters (both field and relation) to a subquery
 * Handles logical operators (_and, _or, _not) that may contain mixed filters
 */
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

  // Handle _and operator
  if (filter._and && Array.isArray(filter._and)) {
    for (const condition of filter._and) {
      // Separate field and relation filters for this condition
      const { fieldFilters, relationFilters } = separateFilters(condition, metadata);

      query.where(function() {
        // Apply field filters
        if (Object.keys(fieldFilters).length > 0) {
          buildWhereClause(this, fieldFilters, tableName, dbType);
        }
      });

      // Apply relation filters
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

  // Handle _or operator
  if (filter._or && Array.isArray(filter._or)) {
    // Build all subqueries first
    const orParts: Array<{fieldFilters: any, subqueries: string[]}> = [];

    for (const condition of filter._or) {
      const { fieldFilters, relationFilters } = separateFilters(condition, metadata);
      const subqueries: string[] = [];

      // Build relation subqueries
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

    // Now apply them synchronously
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

  // Handle _not operator
  if (filter._not) {
    const { fieldFilters, relationFilters } = separateFilters(filter._not, metadata);

    query.whereNot(function() {
      // Apply field filters
      if (Object.keys(fieldFilters).length > 0) {
        buildWhereClause(this, fieldFilters, tableName, dbType);
      }
    });

    // Apply relation filters with NOT EXISTS
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

  // Separate field and relation filters
  const { fieldFilters, relationFilters } = separateFilters(filter, metadata);

  // Apply field filters
  if (Object.keys(fieldFilters).length > 0) {
    buildWhereClause(query, fieldFilters, tableName, dbType);
  }

  // Apply nested relation filters recursively
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

/**
 * Apply relation filters to query using WHERE EXISTS
 *
 * @param knex - Knex instance
 * @param query - Query builder
 * @param filter - Full filter object (may contain both field and relation filters)
 * @param tableName - Main table name
 * @param metadata - Table metadata
 * @param dbType - Database type
 * @param getMetadata - Function to get metadata for nested relations
 */
export async function applyRelationFilters(
  knex: Knex,
  query: Knex.QueryBuilder,
  filter: any,
  tableName: string,
  metadata: TableMetadata,
  dbType: string,
  getMetadata?: (tableName: string) => Promise<TableMetadata | null>,
): Promise<void> {
  // Use applyFiltersToSubquery which handles both field and relation filters
  await applyFiltersToSubquery(knex, query, filter, tableName, metadata, dbType, getMetadata);
}
