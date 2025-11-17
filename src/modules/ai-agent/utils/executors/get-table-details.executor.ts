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
import { optimizeMetadataForLLM } from '../metadata-optimizer.helper';

const logger = new Logger('GetTableDetailsExecutor');

export interface GetTableDetailsExecutorDependencies {
  metadataCacheService: MetadataCacheService;
  queryBuilder: QueryBuilderService;
  tableHandlerService: TableHandlerService;
  queryEngine: QueryEngine;
  routeCacheService: RouteCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  aiConfigCacheService: AiConfigCacheService;
  systemProtectionService: SystemProtectionService;
  tableValidationService: TableValidationService;
  swaggerService: SwaggerService;
  graphqlService: GraphqlService;
}

export async function executeGetTableDetails(
  args: {
    tableName: string[];
    forceRefresh?: boolean;
    id?: (string | number)[];
    name?: string[];
    getData?: boolean;
    fields?: string[];
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

  if (!Array.isArray(args.tableName)) {
    throw new Error('tableName must be an array. For single table, use array with 1 element: ["table_name"]');
  }

  const tableNames = args.tableName;

  if (tableNames.length === 0) {
    throw new Error('At least one table name is required');
  }


  if (args.getData === true && args.id === undefined && args.name === undefined) {
    throw new Error('getData=true requires either id or name parameter. If you only need schema metadata, omit getData parameter. If you need actual table data, provide id (array) or name (array) parameter.');
  }

  const shouldGetData = args.getData === true && (args.id !== undefined || args.name !== undefined);


  if (args.id !== undefined && !Array.isArray(args.id)) {
    throw new Error('id must be an array. For single value, use array with 1 element: [123]');
  }
  if (args.name !== undefined && !Array.isArray(args.name)) {
    throw new Error('name must be an array. For single value, use array with 1 element: ["table_name"]');
  }


  if (args.id && args.id.length !== tableNames.length) {
    throw new Error(`id array length (${args.id.length}) must match tableName array length (${tableNames.length})`);
  }
  if (args.name && args.name.length !== tableNames.length) {
    throw new Error(`name array length (${args.name.length}) must match tableName array length (${tableNames.length})`);
  }

  if (tableNames.length === 1) {
    const tableName = tableNames[0];
    const metadata = await metadataCacheService.getTableMetadata(tableName);
    if (!metadata) {
      throw new Error(`Table ${tableName} not found`);
    }

    let result: any = optimizeMetadataForLLM(metadata);

    if (args.fields && Array.isArray(args.fields) && args.fields.length > 0) {
      const filteredResult: any = {};
      for (const field of args.fields) {
        if (result[field] !== undefined) {
          filteredResult[field] = result[field];
        }
      }
      result = filteredResult;
    }

    if (shouldGetData && context) {
      try {
        const repo = new DynamicRepository({
          context,
          tableName,
          queryBuilder,
          tableHandlerService,
          queryEngine,
          routeCacheService,
          storageConfigCacheService,
          aiConfigCacheService,
          metadataCacheService,
          systemProtectionService,
          tableValidationService,
          bootstrapScriptService: undefined,
          redisPubSubService: undefined,
          swaggerService,
          graphqlService,
        });

        await repo.init();

        let where: any = {};
        const id = args.id ? args.id[0] : undefined;
        const name = args.name ? args.name[0] : undefined;
        if (id !== undefined) {
          where.id = { _eq: id };
        } else if (name !== undefined) {
          where.name = { _eq: name };
        }

        const dataResult = await repo.find({
          where,
          fields: '*',
          limit: 1,
        });

        if (dataResult?.data && dataResult.data.length > 0) {
          result.data = dataResult.data[0];
        } else {
          result.data = null;
        }
      } catch (error: any) {
        logger.error(`[get_table_details] Error fetching data for ${tableName}: ${error.message}`);
        result.dataError = error.message;
      }

    }

    return result;
  }

  const result: Record<string, any> = {};
  const errors: string[] = [];
  const isBulkQuery = tableNames.length > 5;

  for (let i = 0; i < tableNames.length; i++) {
    const tableName = tableNames[i];
    try {
      const metadata = await metadataCacheService.getTableMetadata(tableName);
      if (!metadata) {
        errors.push(`Table ${tableName} not found`);
        continue;
      }
      
      if (isBulkQuery) {
        const optimized = optimizeMetadataForLLM(metadata);
        result[tableName] = {
          name: optimized.name,
          description: optimized.description,
          isSystem: metadata.isSystem || false,
          id: optimized.id,
          columnCount: optimized.columnCount || (optimized.columns?.length || 0),
          relationCount: optimized.relations?.length || 0,
          hasUniques: !!optimized.uniques && optimized.uniques.length > 0,
          hasIndexes: !!optimized.indexes && optimized.indexes.length > 0,
        };
      } else {
        let optimized = optimizeMetadataForLLM(metadata);
        
        if (args.fields && Array.isArray(args.fields) && args.fields.length > 0) {
          const filteredResult: any = {};
          for (const field of args.fields) {
            if (optimized[field] !== undefined) {
              filteredResult[field] = optimized[field];
            }
          }
          result[tableName] = filteredResult;
        } else {
          result[tableName] = optimized;
        }
      }

      if (shouldGetData && context) {
        try {
          const repo = new DynamicRepository({
            context,
            tableName,
            queryBuilder,
            tableHandlerService,
            queryEngine,
            routeCacheService,
            storageConfigCacheService,
            aiConfigCacheService,
            metadataCacheService,
            systemProtectionService,
            tableValidationService,
            bootstrapScriptService: undefined,
            redisPubSubService: undefined,
            swaggerService,
            graphqlService,
          });

          await repo.init();

          let where: any = {};

          const id = args.id ? args.id[i] : undefined;
          const name = args.name ? args.name[i] : undefined;
          
          if (id !== undefined) {
            where.id = { _eq: id };
          } else if (name !== undefined) {
            where.name = { _eq: name };
          }

          const dataResult = await repo.find({
            where,
            fields: '*',
            limit: 1,
          });

          if (dataResult?.data && dataResult.data.length > 0) {
            result[tableName].data = dataResult.data[0];
          } else {
            result[tableName].data = null;
          }
        } catch (error: any) {
          result[tableName].dataError = error.message;
        }
      }
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


