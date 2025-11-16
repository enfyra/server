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
  const { metadataCacheService } = deps;
  const metadata = await metadataCacheService.getTableMetadata(args.tableName);
  if (!metadata) {
    throw new Error(`Table ${args.tableName} not found`);
  }

  const fieldNames = metadata.columns.map((col: any) => col.name);

  return {
    table: args.tableName,
    fields: fieldNames,
    count: fieldNames.length,
  };
}

