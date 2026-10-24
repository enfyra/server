import { getJunctionColumnNames, getJunctionTableName } from '@enfyra/kernel';

export function getSqlJunctionPhysicalNames(input: {
  sourceTable: string;
  propertyName: string;
  targetTable: string;
}) {
  const { sourceColumn, targetColumn } = getJunctionColumnNames(
    input.sourceTable,
    input.propertyName,
    input.targetTable,
  );
  return {
    junctionTableName: getJunctionTableName(
      input.sourceTable,
      input.propertyName,
      input.targetTable,
    ),
    junctionSourceColumn: sourceColumn,
    junctionTargetColumn: targetColumn,
  };
}
