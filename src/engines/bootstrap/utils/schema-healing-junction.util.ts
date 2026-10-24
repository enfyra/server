import type { JunctionPhysicalMetadata } from '../types/schema-healing.types';

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
