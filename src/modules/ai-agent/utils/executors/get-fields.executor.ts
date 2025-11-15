import { Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';

const logger = new Logger('GetFieldsExecutor');

export interface GetFieldsExecutorDependencies {
  metadataCacheService: MetadataCacheService;
}

export async function executeGetFields(
  args: { tableName: string },
  deps: GetFieldsExecutorDependencies,
): Promise<any> {
  logger.debug(`[get_fields] Called with tableName=${args.tableName}`);
  const { metadataCacheService } = deps;
  const metadata = await metadataCacheService.getTableMetadata(args.tableName);
  if (!metadata) {
    logger.debug(`[get_fields] Table ${args.tableName} not found`);
    throw new Error(`Table ${args.tableName} not found`);
  }

  const fieldNames = metadata.columns.map((col: any) => col.name);
  logger.debug(`[get_fields] Returning ${fieldNames.length} fields for ${args.tableName}`);

  return {
    table: args.tableName,
    fields: fieldNames,
    count: fieldNames.length,
  };
}

