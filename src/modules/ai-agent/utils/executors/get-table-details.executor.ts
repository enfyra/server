import { Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../../../infrastructure/cache/services/metadata-cache.service';
import { DynamicRepository } from '../../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../../shared/interfaces/dynamic-context.interface';
import { formatMetadataCompact } from '../compact-format.helper';
import { GetTableDetailsExecutorDependencies } from '../types';

const logger = new Logger('GetTableDetailsExecutor');

export async function executeGetTableDetails(
  args: {
    tableName: string[];
    forceRefresh?: boolean;
  },
  context: TDynamicContext | undefined,
  deps: GetTableDetailsExecutorDependencies,
): Promise<any> {

  const {
    metadataCacheService,
    queryBuilder,
    tableHandlerService,
    queryEngine,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    systemProtectionService,
    tableValidationService,
    swaggerService,
    graphqlService,
  } = deps;

  if (args.forceRefresh) {
    await metadataCacheService.reload();
  }

  if (!args.tableName || !Array.isArray(args.tableName)) {
    return {
      error: true,
      errorCode: 'MISSING_TABLE_NAME',
      message: 'tableName parameter is required and must be an array of table names.',
      suggestion: 'If you do not know the table names, FIRST call find_records to discover tables: find_records({"table":"table_definition","fields":"id,name,isSystem","where":{"name":{"_icontains":"route"}},"limit":5}). Then use the returned table names in get_table_details({"tableName":["route_definition"]}).',
      example: 'get_table_details({"tableName":["route_definition","user_definition"]})',
    };
  }

  const tableNames = args.tableName;

  if (tableNames.length === 0) {
    return {
      error: true,
      errorCode: 'EMPTY_TABLE_NAME',
      message: 'At least one table name is required in the tableName array.',
      suggestion: 'If you do not know the table names, FIRST call find_records to discover tables: find_records({"table":"table_definition","fields":"id,name,isSystem","limit":10}). Then use the returned table names.',
      example: 'get_table_details({"tableName":["route_definition"]})',
    };
  }

  if (tableNames.length === 1) {
    const tableName = tableNames[0];
    const metadata = await metadataCacheService.getTableMetadata(tableName);
    if (!metadata) {
      throw new Error(`Table ${tableName} not found`);
    }

    const result = formatMetadataCompact(metadata);
    return result;
  }

  const result: Record<string, any> = {};
  const errors: string[] = [];

  for (let i = 0; i < tableNames.length; i++) {
    const tableName = tableNames[i];
    try {
      const metadata = await metadataCacheService.getTableMetadata(tableName);
      if (!metadata) {
        errors.push(`Table ${tableName} not found`);
        continue;
      }
      
      result[tableName] = formatMetadataCompact(metadata);
    } catch (error: any) {
      errors.push(`Error loading ${tableName}: ${error.message}`);
    }
  }

  if (errors.length > 0) {
    result._errors = errors;
  }

  if (Object.keys(result).length === 0 && errors.length > 0) {
    result._allFailed = true;
  }

  return result;
}


