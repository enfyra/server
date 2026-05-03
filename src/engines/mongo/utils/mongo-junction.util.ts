import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';

export interface MongoJunctionInfo {
  junctionName: string;
  selfColumn: string;
  otherColumn: string;
  owningTable: string;
  inverseTable: string;
}

export function resolveMongoJunctionInfo(
  currentTable: string,
  relation: {
    type: string;
    propertyName: string;
    targetTable?: string;
    targetTableName?: string;
    mappedBy?: string;
    junctionTableName?: string;
    junctionSourceColumn?: string;
    junctionTargetColumn?: string;
  },
): MongoJunctionInfo | null {
  if (relation.type !== 'many-to-many') return null;
  const targetTable = relation.targetTableName || relation.targetTable;
  if (!targetTable) return null;

  if (relation.mappedBy) {
    const owningTable = targetTable;
    const owningProp = relation.mappedBy;
    const inverseTable = currentTable;
    const junction = getSqlJunctionPhysicalNames({
      sourceTable: owningTable,
      propertyName: owningProp,
      targetTable: inverseTable,
    });
    const junctionName =
      relation.junctionTableName || junction.junctionTableName;

    if (relation.junctionSourceColumn && relation.junctionTargetColumn) {
      return {
        junctionName,
        selfColumn: relation.junctionSourceColumn,
        otherColumn: relation.junctionTargetColumn,
        owningTable,
        inverseTable,
      };
    }

    return {
      junctionName,
      selfColumn: junction.junctionTargetColumn,
      otherColumn: junction.junctionSourceColumn,
      owningTable,
      inverseTable,
    };
  }

  const owningTable = currentTable;
  const inverseTable = targetTable;
  const junction = getSqlJunctionPhysicalNames({
    sourceTable: owningTable,
    propertyName: relation.propertyName,
    targetTable: inverseTable,
  });
  const junctionName =
    relation.junctionTableName || junction.junctionTableName;

  if (relation.junctionSourceColumn && relation.junctionTargetColumn) {
    return {
      junctionName,
      selfColumn: relation.junctionSourceColumn,
      otherColumn: relation.junctionTargetColumn,
      owningTable,
      inverseTable,
    };
  }

  return {
    junctionName,
    selfColumn: junction.junctionSourceColumn,
    otherColumn: junction.junctionTargetColumn,
    owningTable,
    inverseTable,
  };
}
