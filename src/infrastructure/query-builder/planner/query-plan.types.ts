export type DatabaseType = 'postgres' | 'mysql' | 'sqlite' | 'mongodb';
export type RelationType =
  | 'many-to-one'
  | 'one-to-many'
  | 'one-to-one'
  | 'many-to-many';
export type SqlStrategy = 'simple' | 'subquery' | 'cte-flat' | 'cte-aggregate';
export type PaginationPlacement = 'before-joins' | 'after-joins';
export type JoinType = 'left' | 'inner';
export type JoinPurpose = 'data' | 'filter' | 'sort';

export interface JoinSpec {
  id: string;
  propertyName: string;
  parentTable: string;
  targetTable: string;
  relationType: RelationType;
  parentJoinId: string | null;
  joinType: JoinType;
  relationMeta: any;
  purposes: JoinPurpose[];
}

export interface ResolvedSortItem {
  joinId: string | null;
  field: string;
  direction: 'asc' | 'desc';
  fullPath: string;
}

export interface QueryPlan {
  table: string;
  dbType: DatabaseType;
  rawFields: string[] | undefined;
  rawFilter: any | null;
  joins: JoinSpec[];
  sortItems: ResolvedSortItem[];
  limit: number | undefined;
  offset: number | undefined;
  paginationPlacement: PaginationPlacement;
  needsTotalCount: boolean;
  needsFilterCount: boolean;
  hasRelationFilters: boolean;
  hasRelationSort: boolean;
  hasOnlyManyToOneDataJoins: boolean;
  limitedCteFilterJoins: JoinSpec[];
  limitedCteSortJoin: JoinSpec | null;
  sqlStrategy: SqlStrategy;
}
