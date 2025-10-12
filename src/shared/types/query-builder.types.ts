/**
 * Unified query builder types
 * Used by QueryBuilderService for both SQL and MongoDB
 */

export type DatabaseType = 'mysql' | 'postgres' | 'mongodb';

export type WhereOperator = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'like' | 'in' | 'not in' | 'is null' | 'is not null';

export interface WhereCondition {
  field: string;
  operator: WhereOperator;
  value?: any;
}

export interface SortOption {
  field: string;
  direction: 'asc' | 'desc';
}

export interface JoinOption {
  table: string;
  type: 'inner' | 'left' | 'right';
  on: {
    local: string;
    foreign: string;
  };
}

export interface QueryOptions {
  table: string;
  select?: string[];
  where?: WhereCondition[];
  join?: JoinOption[];
  sort?: SortOption[];
  limit?: number;
  offset?: number;
  groupBy?: string[];
  pipeline?: any[]; // MongoDB aggregation pipeline
}

export interface InsertOptions {
  table: string;
  data: Record<string, any> | Record<string, any>[];
  returning?: string[];
}

export interface UpdateOptions {
  table: string;
  where: WhereCondition[];
  data: Record<string, any>;
  returning?: string[];
}

export interface DeleteOptions {
  table: string;
  where: WhereCondition[];
}

export interface CountOptions {
  table: string;
  where?: WhereCondition[];
}

