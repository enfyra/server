import { QueryPlan, SqlStrategy, DatabaseType } from './query-plan.types';

export function decideSqlStrategy(
  plan: QueryPlan,
  dbType: DatabaseType,
): SqlStrategy {
  const hasJoins = plan.joins.length > 0;
  const canUseCTE =
    (dbType === 'postgres' || dbType === 'mysql') &&
    (hasJoins || plan.hasRelationSort) &&
    plan.limit !== undefined;

  if (canUseCTE) {
    return plan.hasOnlyManyToOneDataJoins && !plan.hasRelationSort
      ? 'cte-flat'
      : 'cte-aggregate';
  }
  if (hasJoins) return 'subquery';
  return 'simple';
}
