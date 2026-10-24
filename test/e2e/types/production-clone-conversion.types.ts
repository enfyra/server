export interface ClonePhysicalColumn {
  table_name: string;
  column_name: string;
  data_type: string;
  udt_name: string;
  is_nullable: string;
  column_default: string | null;
  character_maximum_length: number | null;
}

export interface CloneConstraint {
  table_name: string;
  conname: string;
  contype: string;
  columns: string[];
  target_table: string | null;
  target_columns: string[];
  confupdtype: string;
  confdeltype: string;
}

export interface CloneCatalog {
  columns: ClonePhysicalColumn[];
  constraints: CloneConstraint[];
  indexes: Array<{ tablename: string; indexname: string; indexdef: string }>;
  enums: Array<{ typname: string; labels: string[] }>;
  tables: Array<Record<string, any>>;
  metadataColumns: Array<Record<string, any>>;
  relations: Array<Record<string, any>>;
}

export interface CloneConversionState {
  sourceDatabase: string;
  mysqlDatabase: string;
  mongoDatabase: string;
  directory: string;
}
