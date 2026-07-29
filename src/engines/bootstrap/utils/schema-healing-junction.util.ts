import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';
import type {
  JunctionPhysicalMetadata,
  SchemaHealingSnapshot,
} from '../types/schema-healing.types';

export function getTargetJunctionContract(
  snapshot: SchemaHealingSnapshot,
  input: {
    sourceTable: string;
    propertyName: string;
    targetTable: string;
  },
): JunctionPhysicalMetadata {
  const standard = getSqlJunctionPhysicalNames(input);
  const relation = snapshot?.[input.sourceTable]?.relations?.find(
    (entry: any) =>
      entry.propertyName === input.propertyName &&
      entry.targetTable === input.targetTable,
  );
  return {
    junctionTableName:
      relation?.junctionTableName || standard.junctionTableName,
    junctionSourceColumn:
      relation?.junctionSourceColumn || standard.junctionSourceColumn,
    junctionTargetColumn:
      relation?.junctionTargetColumn || standard.junctionTargetColumn,
  };
}

export function diffJunctionMetadata(
  relation: any,
  expected: JunctionPhysicalMetadata,
): Partial<JunctionPhysicalMetadata> {
  const update: Partial<JunctionPhysicalMetadata> = {};
  if (relation.junctionTableName !== expected.junctionTableName) {
    update.junctionTableName = expected.junctionTableName;
  }
  if (relation.junctionSourceColumn !== expected.junctionSourceColumn) {
    update.junctionSourceColumn = expected.junctionSourceColumn;
  }
  if (relation.junctionTargetColumn !== expected.junctionTargetColumn) {
    update.junctionTargetColumn = expected.junctionTargetColumn;
  }
  return update;
}
