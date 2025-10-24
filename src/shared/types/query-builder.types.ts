export type DatabaseType = 'mysql' | 'postgres' | 'mongodb' | 'sqlite';

export type WhereOperator = '=' | '!=' | '>' | '<' | '>=' | '<=' | 'like' | 'in' | 'not in' | 'is null' | 'is not null' | '_contains' | '_starts_with' | '_ends_with' | '_between' | '_is_null' | '_is_not_null';

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
  select?: string[]; // Raw column selection (old way)
  fields?: string[]; // Smart field selection with auto-relation expansion (new way)
  where?: WhereCondition[];
  join?: JoinOption[];
  sort?: SortOption[];
  limit?: number;
  offset?: number;
  groupBy?: string[];
  pipeline?: any[]; // MongoDB aggregation pipeline
  mongoFieldsExpanded?: { // MongoDB expanded fields from expandFieldsMongo()
    scalarFields: string[];
    relations: Array<{
      propertyName: string;
      targetTable: string;
      localField: string;
      foreignField: string;
      type: 'one' | 'many';
      nestedFields?: string[];
    }>;
  };
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

