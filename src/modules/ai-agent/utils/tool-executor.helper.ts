import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { SwaggerService } from '../../../infrastructure/swagger/services/swagger.service';
import { GraphqlService } from '../../graphql/services/graphql.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { optimizeMetadataForLLM } from './metadata-optimizer.helper';

export class ToolExecutor {
  constructor(
    private readonly metadataCacheService: MetadataCacheService,
    private readonly queryBuilder: QueryBuilderService,
    private readonly tableHandlerService: TableHandlerService,
    private readonly queryEngine: QueryEngine,
    private readonly routeCacheService: RouteCacheService,
    private readonly storageConfigCacheService: StorageConfigCacheService,
    private readonly aiConfigCacheService: AiConfigCacheService,
    private readonly systemProtectionService: SystemProtectionService,
    private readonly tableValidationService: TableValidationService,
    private readonly swaggerService: SwaggerService,
    private readonly graphqlService: GraphqlService,
  ) {}

  async executeTool(
    toolCall: {
      id: string;
      function: {
        name: string;
        arguments: string;
      };
    },
    context: TDynamicContext,
  ): Promise<any> {
    const { name, arguments: argsStr } = toolCall.function;
    let args: any;

    try {
      args = JSON.parse(argsStr);
    } catch (e) {
      throw new Error(`Invalid tool arguments: ${argsStr}`);
    }

    switch (name) {
      case 'get_metadata':
        return await this.executeGetMetadata(args);
      case 'get_table_details':
        return await this.executeGetTableDetails(args);
      case 'get_hint':
        return await this.executeGetHint(args, context);
      case 'dynamic_repository':
        return await this.executeDynamicRepository(args, context);
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  private async executeGetMetadata(args: { forceRefresh?: boolean }): Promise<any> {
    if (args.forceRefresh) {
      await this.metadataCacheService.reload();
    }

    const metadata = await this.metadataCacheService.getMetadata();
    
    const tablesSummary = Array.from(metadata.tables.entries()).map(([name, table]) => ({
      name,
      description: table.description || '',
      isSingleRecord: table.isSingleRecord || false,
    }));
    
    return {
      tables: tablesSummary,
      tablesList: metadata.tablesList,
    };
  }

  private async executeGetTableDetails(args: { tableName: string; forceRefresh?: boolean }): Promise<any> {
    if (args.forceRefresh) {
      await this.metadataCacheService.reload();
    }

    const metadata = await this.metadataCacheService.getTableMetadata(args.tableName);
    if (!metadata) {
      throw new Error(`Table ${args.tableName} not found`);
    }

    return optimizeMetadataForLLM(metadata);
  }

  private async executeGetHint(args: { category?: string }, context: TDynamicContext): Promise<any> {
    const dbType = this.queryBuilder.getDbType();
    const isMongoDB = dbType === 'mongodb';
    const idFieldName = isMongoDB ? '_id' : 'id';

    let dbTypeContent = `Current database type: ${dbType}\n\n`;
    if (isMongoDB) {
      dbTypeContent += `**MongoDB:**
- Primary key: "_id" (not "id")
- Relations: use "{_id: value}"
- createdAt/updatedAt auto-added
- Table names: check get_metadata first (not all have "_definition" suffix)`;
    } else {
      dbTypeContent += `**SQL (${dbType}):**
- Primary key: "id"
- Relations: use "{id: value}"
- createdAt/updatedAt auto-added
- Table names: check get_metadata first (not all have "_definition" suffix)`;
    }

    const dbTypeHint = {
      category: 'database_type',
      title: 'Database Type Information',
      content: dbTypeContent,
    };

    const relationContent = `**Relations:**
- Use propertyName (NOT FK column names like mainTableId, categoryId, userId)
- targetTable must be object: {"${idFieldName}": value}, NOT string
- M2O: {"category": {"${idFieldName}": 1}} or {"category": 1}
- O2O: {"profile": {"${idFieldName}": 5}} or {"profile": {new_data}}
- M2M: {"tags": [{"${idFieldName}": 1}, {"${idFieldName}": 2}, 3]}
- O2M: {"items": [{"${idFieldName}": 10, qty: 5}, {new_item}]}

**Error Handling:**
- If error returned: STOP, report to user immediately
- Delete requires id (not where), find record first if needed`;

    const relationHint = {
      category: 'relations',
      title: 'Relation Behavior in Enfyra',
      content: relationContent,
    };

    return {
      dbType,
      isMongoDB,
      idField: idFieldName,
      hints: [dbTypeHint, relationHint],
      count: 2,
    };
  }

  private async executeDynamicRepository(
    args: {
      table: string;
      operation: 'find' | 'create' | 'update' | 'delete';
      where?: any;
      fields?: string;
      data?: any;
      id?: string | number;
    },
    context: TDynamicContext,
  ): Promise<any> {
    const repo = new DynamicRepository({
      context,
      tableName: args.table,
      queryBuilder: this.queryBuilder,
      tableHandlerService: this.tableHandlerService,
      queryEngine: this.queryEngine,
      routeCacheService: this.routeCacheService,
      storageConfigCacheService: this.storageConfigCacheService,
      aiConfigCacheService: this.aiConfigCacheService,
      metadataCacheService: this.metadataCacheService,
      systemProtectionService: this.systemProtectionService,
      tableValidationService: this.tableValidationService,
      bootstrapScriptService: undefined,
      redisPubSubService: undefined,
      swaggerService: this.swaggerService,
      graphqlService: this.graphqlService,
    });

    await repo.init();

    try {
      switch (args.operation) {
        case 'find':
          return await repo.find({
            where: args.where,
            fields: args.fields,
          });
        case 'create':
          if (!args.data) {
            throw new Error('data is required for create operation');
          }
          return await repo.create(args.data);
        case 'update':
          if (!args.id) {
            throw new Error('id is required for update operation');
          }
          if (!args.data) {
            throw new Error('data is required for update operation');
          }
          return await repo.update(args.id, args.data);
        case 'delete':
          if (!args.id) {
            throw new Error('id is required for delete operation');
          }
          return await repo.delete(args.id);
        default:
          throw new Error(`Unknown operation: ${args.operation}`);
      }
    } catch (error: any) {
      const errorMessage = error?.message || error?.response?.message || String(error);
      const errorCode = error?.errorCode || error?.response?.errorCode || 'UNKNOWN_ERROR';
      const details = error?.details || error?.response?.details || {};
      
      if (errorMessage.includes('already exists') || errorMessage.includes('duplicate')) {
        return {
          error: true,
          errorCode: 'RESOURCE_EXISTS',
          message: errorMessage,
          suggestion: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! The resource already exists. You MUST report this to the user immediately and ask how to proceed. DO NOT call any more tools.',
          details,
        };
      }

      if (errorMessage.includes('not found') || errorMessage.includes('does not exist')) {
        return {
          error: true,
          errorCode: 'RESOURCE_NOT_FOUND',
          message: errorMessage,
          suggestion: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! The resource does not exist. You MUST report this to the user immediately and ask how to proceed. DO NOT call any more tools.',
          details,
        };
      }

      return {
        error: true,
        errorCode,
        message: errorMessage,
        suggestion: '🛑 CRITICAL: STOP ALL OPERATIONS NOW! An error occurred. You MUST report this to the user immediately and ask how to proceed. DO NOT call any more tools.',
        details,
      };
    }
  }
}

