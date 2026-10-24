export interface SqlJunctionMetadataInput {
  sourceTable: string;
  propertyName: string;
  targetTable: string;
}

export interface SqlJunctionMetadata {
  junctionTable: string;
  sourceColumn: string;
  targetColumn: string;
}
