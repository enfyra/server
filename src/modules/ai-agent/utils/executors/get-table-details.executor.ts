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
    id?: string | number;
    name?: string;
    getData?: boolean;
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

  const shouldGetData = args.getData === true && (args.id !== undefined || args.name !== undefined);

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
        if (args.id !== undefined) {
          where.id = { _eq: args.id };
        } else if (args.name !== undefined) {
          where.name = { _eq: args.name };
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
        result.dataError = error.message;
      }
    }

    return result;
  }

  const result: Record<string, any> = {};
  const errors: string[] = [];

  for (const tableName of tableNames) {
    try {
      const metadata = await metadataCacheService.getTableMetadata(tableName);
      if (!metadata) {
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
          if (args.id !== undefined) {
            where.id = { _eq: args.id };
          } else if (args.name !== undefined) {
            where.name = { _eq: args.name };
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

