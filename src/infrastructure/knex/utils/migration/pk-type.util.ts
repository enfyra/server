import { Knex } from 'knex';
import { Logger } from '@nestjs/common';

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

    const pkInfo = await knex('column_definition')
      .join(
        'table_definition',
        'column_definition.table',
        '=',
        'table_definition.id',
      )
      .where('table_definition.name', tableName)
      .where('column_definition.isPrimary', true)
      .select('column_definition.type')
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
      `Error getting primary key type for ${tableName}: ${error.message}, defaulting to int`,
    );
    return 'int';
  }
}
