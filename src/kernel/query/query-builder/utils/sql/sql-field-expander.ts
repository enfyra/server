import { Knex } from 'knex';
import {
  quoteIdentifier,
  getPrimaryKeyColumn,
} from '../../../../../engine/knex';
import { getForeignKeyColumnName } from '../../../query-dsl/utils/sql-schema-naming.util';

export async function expandFieldsToSelect(
  knex: Knex,
  tableName: string,
  fields: string[],
  metadataGetter: any,
  dbType: 'postgres' | 'mysql' | 'sqlite',
  maxQueryDepth?: number,
  deepOptions?: Record<string, any>,
): Promise<{
  select: string[];
  batchFetchDescriptors?: any[];
}> {
  if (!metadataGetter) {
    return { select: fields };
  }

  try {
    const { expandFieldsToJoinsAndSelect } =
      await import('../sql/expand-fields');
    const expanded = await expandFieldsToJoinsAndSelect(
      tableName,
      fields,
      metadataGetter,
      dbType,
      maxQueryDepth,
      deepOptions,
    );
    return {
      select: expanded.select,
      batchFetchDescriptors: expanded.batchFetchDescriptors,
    };
  } catch (error) {
    return { select: fields };
  }
}

export function getMetadataGetter(
  metadata: any,
): ((tName: string) => Promise<any>) | null {
  const allMetadata = metadata;
  if (!allMetadata) return null;
  return async (tName: string) => {
    const tableMeta = allMetadata.tables?.get(tName);
    if (!tableMeta) return null;
    return {
      name: tableMeta.name,
      columns: (tableMeta.columns || []).map((col: any) => ({
        name: col.name,
        type: col.type,
      })),
      relations: tableMeta.relations || [],
    };
  };
}

export function buildRelationSortSubquery(
  relationMeta: any,
  sortField: string,
  parentTable: string,
  metadata: any,
  dbType: 'postgres' | 'mysql' | 'sqlite',
): string | null {
  const targetTable = relationMeta.targetTableName || relationMeta.targetTable;
  if (!targetTable) return null;

  const fkCol =
    relationMeta.foreignKeyColumn || getForeignKeyColumnName(targetTable);
  if (!fkCol) return null;

  const q = (s: string) => quoteIdentifier(s, dbType);

  const targetMeta = metadata?.tables?.get(targetTable);
  const pkCol = targetMeta ? getPrimaryKeyColumn(targetMeta) : null;
  const targetPk = pkCol?.name || 'id';

  return `(SELECT ${q(targetTable)}.${q(sortField)} FROM ${q(targetTable)} WHERE ${q(targetTable)}.${q(targetPk)} = ${q(parentTable)}.${q(fkCol)})`;
}
