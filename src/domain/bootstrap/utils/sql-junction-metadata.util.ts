import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import type {
  SqlJunctionMetadata,
  SqlJunctionMetadataInput,
} from '../types/sql-junction-metadata.types';

export async function getSqlJunctionMetadata(
  queryBuilderService: IQueryBuilder,
  input: SqlJunctionMetadataInput,
): Promise<SqlJunctionMetadata> {
  const knex = queryBuilderService.getKnex();
  const relation = await knex('enfyra_relation as r')
    .leftJoin(
      'enfyra_table as sourceTable',
      'r.sourceTableId',
      'sourceTable.id',
    )
    .leftJoin(
      'enfyra_table as targetTable',
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
  if (
    !relation?.junctionTableName ||
    !relation.junctionSourceColumn ||
    !relation.junctionTargetColumn
  ) {
    throw new Error(
      `Junction metadata is missing or incomplete for ${input.sourceTable}.${input.propertyName}`,
    );
  }
  return {
    junctionTable: relation.junctionTableName,
    sourceColumn: relation.junctionSourceColumn,
    targetColumn: relation.junctionTargetColumn,
  };
}
