import { FilterNode } from './types/filter-ast';
import { FieldTree } from './types/field-tree';
export type DatabaseType = 'postgres' | 'mysql' | 'sqlite' | 'mongodb';
export type RelationType =
  | 'many-to-one'
  | 'one-to-many'
  | 'one-to-one'
  | 'many-to-many';
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
  needsTotalCount: boolean;
  needsFilterCount: boolean;
  hasRelationFilters: boolean;
  hasRelationSort: boolean;
  filterTree: FilterNode | null;
  fieldTree: FieldTree | null;
}
