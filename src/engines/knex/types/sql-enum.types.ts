export interface SqlEnumColumnDefinition {
  name: string;
  type?: string;
  options?: unknown;
}

export interface PostgresEnumTypePlan {
  typeName: string;
  statements: string[];
}
