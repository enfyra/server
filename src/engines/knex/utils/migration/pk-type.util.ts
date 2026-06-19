import { Knex } from 'knex';
import { Logger } from '../../../../shared/logger';
import { getErrorMessage } from '../../../../shared/utils/error.util';
const logger = new Logger('PkTypeUtil');

export async function getPrimaryKeyTypeForTable(
  knex: Knex,
  tableName: string,
  metadataCacheService?: any,
): Promise<'uuid' | 'int'> {
  try {
    if (metadataCacheService) {
      const targetMetadata =
        await metadataCacheService.lookupTableByName(tableName);
      if (targetMetadata) {
        const pkColumn = targetMetadata.columns.find((c: any) => c.isPrimary);
        if (pkColumn) {
          const type = pkColumn.type?.toLowerCase() || '';
          return type === 'uuid' || type === 'uuidv4' || type.includes('uuid')
            ? 'uuid'
            : 'int';
        }
      }
    }

    const pkInfo = await knex('enfyra_column')
      .join(
        'enfyra_table',
        'enfyra_column.table',
        '=',
        'enfyra_table.id',
      )
      .where('enfyra_table.name', tableName)
      .where('enfyra_column.isPrimary', true)
      .select('enfyra_column.type')
      .first();

    if (pkInfo) {
      const type = pkInfo.type?.toLowerCase() || '';
      return type === 'uuid' || type === 'uuidv4' || type.includes('uuid')
        ? 'uuid'
        : 'int';
    }

    logger.warn(
      `Could not find primary key for table ${tableName}, defaulting to int`,
    );
    return 'int';
  } catch (error) {
    logger.warn(
      `Error getting primary key type for ${tableName}: ${getErrorMessage(error)}, defaulting to int`,
    );
    return 'int';
  }
}
