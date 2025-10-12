import { Knex } from 'knex';

/**
 * Execute count queries for meta information
 */
export async function executeCountQueries(
  knex: Knex,
  query: Knex.QueryBuilder,
  tableName: string,
  metaParts: string[],
  hasFilter: boolean
): Promise<{ totalCount?: number; filterCount?: number }> {
  const result: { totalCount?: number; filterCount?: number } = {};

  if (metaParts.includes('totalCount') || metaParts.includes('*')) {
    const totalCountResult = await knex(tableName).count('* as count').first();
    result.totalCount = Number(totalCountResult?.count || 0);
  }

  // filterCount = count after filter, before pagination (always run if requested)
  if (metaParts.includes('filterCount') || metaParts.includes('*')) {
    // Clone query without select/limit for count
    const countQuery = query.clone().clearSelect().clearOrder().limit(1);
    const filterCountResult = await countQuery
      .countDistinct(`${tableName}.id as count`)
      .first();
    result.filterCount = Number(filterCountResult?.count || 0);
  }

  return result;
}


