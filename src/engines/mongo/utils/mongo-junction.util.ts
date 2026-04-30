import {
  getJunctionColumnNames,
  getJunctionTableName,
} from '@enfyra/kernel';

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
    const junctionName =
      relation.junctionTableName ||
      getJunctionTableName(owningTable, owningProp, inverseTable);

    if (relation.junctionSourceColumn && relation.junctionTargetColumn) {
      return {
        junctionName,
        selfColumn: relation.junctionSourceColumn,
        otherColumn: relation.junctionTargetColumn,
        owningTable,
        inverseTable,
      };
    }

    const cols = getJunctionColumnNames(owningTable, owningProp, inverseTable);
    return {
      junctionName,
      selfColumn: cols.targetColumn,
      otherColumn: cols.sourceColumn,
      owningTable,
      inverseTable,
    };
  }

  const owningTable = currentTable;
  const inverseTable = targetTable;
  const junctionName =
    relation.junctionTableName ||
    getJunctionTableName(owningTable, relation.propertyName, inverseTable);

  if (relation.junctionSourceColumn && relation.junctionTargetColumn) {
    return {
      junctionName,
      selfColumn: relation.junctionSourceColumn,
      otherColumn: relation.junctionTargetColumn,
      owningTable,
      inverseTable,
    };
  }

  const cols = getJunctionColumnNames(
    owningTable,
    relation.propertyName,
    inverseTable,
  );

  return {
    junctionName,
    selfColumn: cols.sourceColumn,
    otherColumn: cols.targetColumn,
    owningTable,
    inverseTable,
  };
}
