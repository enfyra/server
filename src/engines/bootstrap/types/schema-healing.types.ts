export type SchemaHealingSnapshot = Record<
  string,
  {
    name?: string;
    isSystem?: boolean;
    columns?: any[];
    relations?: any[];
    [key: string]: any;
  }
>;

export interface JunctionPhysicalMetadata {
  junctionTableName: string;
  junctionSourceColumn: string;
  junctionTargetColumn: string;
}
