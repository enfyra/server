import type { JunctionPhysicalMetadata } from './schema-healing.types';

export interface JunctionHealingInput extends JunctionPhysicalMetadata {
  sourceTable: string;
  targetTable: string;
  propertyName: string;
  previousTableName?: string | null;
  previousSourceColumn?: string | null;
  previousTargetColumn?: string | null;
}

export interface JunctionColumnMapping {
  sourceColumn: string;
  targetColumn: string;
  sourceAliases: string[];
  targetAliases: string[];
}

export interface SqlJunctionForeignKey {
  constraintName: string;
  columnName: string;
  targetTable: string;
  targetColumn: string;
  onDelete: string;
  onUpdate: string;
}

export interface SqlJunctionPrimaryKey {
  constraintName: string;
  columnName: string;
}
