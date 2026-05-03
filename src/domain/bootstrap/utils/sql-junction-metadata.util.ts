import {
  getForeignKeyColumnName,
  getJunctionTableName,
} from '@enfyra/kernel';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';

export async function getSqlJunctionMetadata(
  queryBuilderService: IQueryBuilder,
  input: {
    sourceTable: string;
    propertyName: string;
    targetTable: string;
  },
): Promise<{
  junctionTable: string;
  sourceColumn: string;
  targetColumn: string;
}> {
  const knex = queryBuilderService.getKnex();
  const relation = await knex('relation_definition as r')
    .leftJoin(
      'table_definition as sourceTable',
      'r.sourceTableId',
      'sourceTable.id',
    )
    .leftJoin(
      'table_definition as targetTable',
      'r.targetTableId',
      'targetTable.id',
    )
    .select(
      'r.junctionTableName as junctionTableName',
      'r.junctionSourceColumn as junctionSourceColumn',
      'r.junctionTargetColumn as junctionTargetColumn',
    )
    .where('sourceTable.name', input.sourceTable)
    .where('targetTable.name', input.targetTable)
    .where('r.propertyName', input.propertyName)
    .first();

  return {
    junctionTable:
      relation?.junctionTableName ||
      getJunctionTableName(
        input.sourceTable,
        input.propertyName,
        input.targetTable,
      ),
    sourceColumn:
      relation?.junctionSourceColumn ||
      getForeignKeyColumnName(input.sourceTable),
    targetColumn:
      relation?.junctionTargetColumn ||
      getForeignKeyColumnName(input.targetTable),
  };
}
