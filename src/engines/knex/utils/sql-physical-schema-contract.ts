import {
  getForeignKeyColumnName,
  getShortFkConstraintName,
  getShortIndexName,
  getShortPkName,
} from '../../../kernel/query';
import type {
  ColumnDef,
  JunctionTableDef,
  RelationDef,
  TableDef,
} from '../../../shared/types/database-init.types';
import type {
  SqlForeignKeyContract,
  SqlJunctionTableContract,
  SqlPhysicalIndexContract,
  SqlPhysicalUniqueContract,
  SqlRelationOnDelete,
} from '../types/sql-physical-schema-contract.types';

type RelationLike = RelationDef & {
  targetTableName?: string;
  foreignKeyColumn?: string;
  mappedBy?: string;
  onDelete?: string | null;
  junctionTableName?: string;
  junctionSourceColumn?: string;
  junctionTargetColumn?: string;
  _isInverseGenerated?: boolean;
};

type TableLike = TableDef & {
  targetTableName?: string;
};

const RELATION_FK_TYPES = new Set(['many-to-one', 'one-to-one']);
const TEMPORAL_TYPES = new Set(['date', 'datetime', 'timestamp']);

export function isSqlForeignKeyRelation(relation: RelationLike): boolean {
  if (!RELATION_FK_TYPES.has(relation.type)) return false;
  if (relation.type === 'one-to-one' && relation._isInverseGenerated) {
    return false;
  }
  return true;
}

export function resolveSqlRelationOnDelete(rel: {
  onDelete?: string | null;
  isNullable?: boolean | number | null;
}): SqlRelationOnDelete {
  if (
    rel.onDelete === 'CASCADE' ||
    rel.onDelete === 'SET NULL' ||
    rel.onDelete === 'RESTRICT'
  ) {
    return rel.onDelete;
  }
  return rel.isNullable === false || rel.isNullable === 0
    ? 'RESTRICT'
    : 'SET NULL';
}

export function getSqlRelationForeignKeyColumn(relation: {
  propertyName: string;
  foreignKeyColumn?: string | null;
}): string {
  return relation.foreignKeyColumn || getForeignKeyColumnName(relation.propertyName);
}

export function getSqlRelationTargetTable(relation: {
  targetTable?: string;
  targetTableName?: string;
}): string {
  return relation.targetTableName || relation.targetTable || '';
}

export function buildSqlForeignKeyContracts(
  tableName: string,
  relations: RelationLike[] = [],
): SqlForeignKeyContract[] {
  return relations
    .filter(isSqlForeignKeyRelation)
    .map((relation) => ({
      tableName,
      propertyName: relation.propertyName,
      columnName: getSqlRelationForeignKeyColumn(relation),
      targetTable: getSqlRelationTargetTable(relation),
      targetColumn: 'id',
      onDelete: resolveSqlRelationOnDelete(relation),
      onUpdate: 'CASCADE',
      nullable: relation.isNullable !== false,
    }));
}

export function resolveSqlPhysicalColumns(
  logicalColumns: string[] = [],
  relations: RelationLike[] = [],
): string[] {
  return logicalColumns.map((fieldName) => {
    const relation = relations.find((r) => r.propertyName === fieldName);
    if (relation && isSqlForeignKeyRelation(relation)) {
      return getSqlRelationForeignKeyColumn(relation);
    }
    return fieldName;
  });
}

export function buildSqlUniqueContracts(
  tableName: string,
  table: Pick<TableLike, 'uniques' | 'relations'>,
): SqlPhysicalUniqueContract[] {
  return (table.uniques || [])
    .filter((group) => Array.isArray(group) && group.length > 0)
    .map((group) => {
      const physicalColumns = resolveSqlPhysicalColumns(
        group,
        (table.relations || []) as RelationLike[],
      );
      return {
        name: `uq_${tableName}_${physicalColumns.join('_')}`,
        logicalColumns: group,
        physicalColumns,
      };
    });
}

export function buildSqlIndexContracts(
  tableName: string,
  table: Pick<TableLike, 'indexes' | 'relations' | 'columns'>,
): SqlPhysicalIndexContract[] {
  const contracts: SqlPhysicalIndexContract[] = [];
  const seen = new Set<string>();
  const add = (
    logicalColumns: string[],
    physicalColumns: string[],
    source: SqlPhysicalIndexContract['source'],
    nameColumns = logicalColumns,
  ) => {
    if (physicalColumns.length === 0) return;
    const physicalWithTieBreaker = physicalColumns.includes('id')
      ? physicalColumns
      : [...physicalColumns, 'id'];
    const key = physicalWithTieBreaker.join('|').toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    contracts.push({
      name: `idx_${tableName}_${nameColumns.join('_')}`,
      logicalColumns,
      physicalColumns: physicalWithTieBreaker,
      source,
    });
  };

  for (const group of table.indexes || []) {
    if (!Array.isArray(group) || group.length === 0) continue;
    const physicalColumns = resolveSqlPhysicalColumns(
      group,
      (table.relations || []) as RelationLike[],
    );
    add(group, physicalColumns, 'metadata', physicalColumns);
  }

  for (const fk of buildSqlForeignKeyContracts(
    tableName,
    (table.relations || []) as RelationLike[],
  )) {
    add([fk.propertyName], [fk.columnName], 'relation-fk', [fk.columnName]);
  }

  for (const col of table.columns || []) {
    if (col.name === 'id') continue;
    if (!col.name.endsWith('Id')) continue;
    add([col.name], [col.name], 'id-suffix-column');
  }

  add(['createdAt'], ['createdAt'], 'system-timestamp');
  add(['updatedAt'], ['updatedAt'], 'system-timestamp');

  for (const col of table.columns || []) {
    if (TEMPORAL_TYPES.has(col.type) && !['createdAt', 'updatedAt'].includes(col.name)) {
      add([col.name], [col.name], 'temporal-column');
    }
  }

  return contracts;
}

export function getSqlAutoGeneratedIndexLogicalGroups(
  tableName: string,
  table: Pick<TableLike, 'indexes' | 'relations' | 'columns'>,
): string[][] {
  return buildSqlIndexContracts(tableName, table)
    .filter((idx) => idx.source !== 'metadata')
    .map((idx) => idx.logicalColumns);
}

export function buildSqlJunctionTableContract(
  junction: JunctionTableDef,
): SqlJunctionTableContract {
  return {
    tableName: junction.tableName,
    sourceTable: junction.sourceTable,
    targetTable: junction.targetTable,
    sourceColumn: junction.sourceColumn,
    targetColumn: junction.targetColumn,
    primaryKeyName: getShortPkName(junction.tableName),
    sourceForeignKeyName: getShortFkConstraintName(
      junction.tableName,
      junction.sourceColumn,
      'src',
    ),
    targetForeignKeyName: getShortFkConstraintName(
      junction.tableName,
      junction.targetColumn,
      'tgt',
    ),
    sourceIndexName: getShortIndexName(
      junction.sourceTable,
      junction.sourcePropertyName,
      'src',
    ),
    targetIndexName: getShortIndexName(
      junction.sourceTable,
      junction.sourcePropertyName,
      'tgt',
    ),
    reverseIndexName: getShortIndexName(
      junction.sourceTable,
      junction.sourcePropertyName,
      'rev',
    ),
    onDelete: 'CASCADE',
    onUpdate: 'CASCADE',
  };
}

export function buildSqlJunctionTableContractFromRelation(
  sourceTable: string,
  relation: RelationLike,
): SqlJunctionTableContract | null {
  const targetTable = getSqlRelationTargetTable(relation);
  if (
    relation.type !== 'many-to-many' ||
    !relation.junctionTableName ||
    !relation.junctionSourceColumn ||
    !relation.junctionTargetColumn ||
    !targetTable
  ) {
    return null;
  }
  return buildSqlJunctionTableContract({
    tableName: relation.junctionTableName,
    sourceTable,
    targetTable,
    sourceColumn: relation.junctionSourceColumn,
    targetColumn: relation.junctionTargetColumn,
    sourcePropertyName: relation.propertyName,
  });
}
