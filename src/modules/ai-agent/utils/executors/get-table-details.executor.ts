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
  },
  context: TDynamicContext | undefined,
  deps: GetTableDetailsExecutorDependencies,
): Promise<any> {
  logger.debug(`[get_table_details] Called with tableName=${JSON.stringify(args.tableName)}, getData=${args.getData}, forceRefresh=${args.forceRefresh}, hasId=${!!args.id}, hasName=${!!args.name}`);

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
    logger.debug(`[get_table_details] Force refreshing metadata`);
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

    const result: any = optimizeMetadataForLLM(metadata);

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

        logger.debug(`[get_table_details] Fetching data for ${tableName}`);
        const dataResult = await repo.find({
          where,
          fields: '*',
          limit: 1,
        });

        if (dataResult?.data && dataResult.data.length > 0) {
          result.data = dataResult.data[0];
          logger.debug(`[get_table_details] Found data for ${tableName}`);
        } else {
          result.data = null;
          logger.debug(`[get_table_details] No data found for ${tableName}`);
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

  for (let i = 0; i < tableNames.length; i++) {
    const tableName = tableNames[i];
    try {
      logger.debug(`[get_table_details] Processing table ${i + 1}/${tableNames.length}: ${tableName}`);
      const metadata = await metadataCacheService.getTableMetadata(tableName);
      if (!metadata) {
        logger.debug(`[get_table_details] Table ${tableName} not found`);
        errors.push(`Table ${tableName} not found`);
        continue;
      }
      result[tableName] = optimizeMetadataForLLM(metadata);

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


