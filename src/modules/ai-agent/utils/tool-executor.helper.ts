import { Logger } from '@nestjs/common';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
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
import { ConversationService } from '../services/conversation.service';
import { executeGetTableDetails } from './executors/get-table-details.executor';
import { executeGetHint } from './executors/get-hint.executor';
import { executeCreateTables } from './executors/create-tables.executor';
import { executeUpdateTables } from './executors/update-tables.executor';
import { executeDeleteTables } from './executors/delete-tables.executor';
import { executeUpdateTask } from './executors/update-task.executor';
import { executeGetTask } from './executors/get-task.executor';
import { executeDynamicRepository } from './executors/dynamic-repository.executor';
import { executeBatchDynamicRepository } from './executors/batch-dynamic-repository.executor';

export class ToolExecutor {
  private readonly logger = new Logger(ToolExecutor.name);

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
    private readonly conversationService: ConversationService,
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
    abortSignal?: AbortSignal,
  ): Promise<any> {
    if (abortSignal?.aborted) {
      return {
        error: true,
        errorCode: 'REQUEST_ABORTED',
        message: 'Request aborted by client',
      };
    }
    const { name, arguments: argsStr } = toolCall.function;
    let args: any;

    try {
      args = JSON.parse(argsStr);
    } catch (e: any) {
      this.logger.error(
        `[ToolExecutor] Failed to parse args for ${name}`,
        {
          toolCallId: toolCall.id,
          toolName: name,
          rawArgs: argsStr?.substring(0, 500) || '',
          error: e?.message || String(e),
        },
      );
      throw new Error(`Invalid tool arguments: ${argsStr}`);
    }


    let result: any;

    switch (name) {
      case 'get_table_details':
        result = await executeGetTableDetails(args, context, {
          metadataCacheService: this.metadataCacheService,
          queryBuilder: this.queryBuilder,
          tableHandlerService: this.tableHandlerService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          storageConfigCacheService: this.storageConfigCacheService,
          aiConfigCacheService: this.aiConfigCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
          swaggerService: this.swaggerService,
          graphqlService: this.graphqlService,
        });
        break;
      case 'get_hint':
        result = await executeGetHint(args, context, {
          queryBuilder: this.queryBuilder,
        });
        break;
      case 'create_tables':
        result = await executeCreateTables(args, context, abortSignal, {
          metadataCacheService: this.metadataCacheService,
          queryBuilder: this.queryBuilder,
          tableHandlerService: this.tableHandlerService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          storageConfigCacheService: this.storageConfigCacheService,
          aiConfigCacheService: this.aiConfigCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
          swaggerService: this.swaggerService,
          graphqlService: this.graphqlService,
        });
        break;
      case 'update_tables':
        result = await executeUpdateTables(args, context, abortSignal, {
          metadataCacheService: this.metadataCacheService,
          queryBuilder: this.queryBuilder,
          tableHandlerService: this.tableHandlerService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          storageConfigCacheService: this.storageConfigCacheService,
          aiConfigCacheService: this.aiConfigCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
          swaggerService: this.swaggerService,
          graphqlService: this.graphqlService,
        });
        break;
      case 'delete_tables':
        result = await executeDeleteTables(args, context, abortSignal, {
          metadataCacheService: this.metadataCacheService,
          queryBuilder: this.queryBuilder,
          tableHandlerService: this.tableHandlerService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          storageConfigCacheService: this.storageConfigCacheService,
          aiConfigCacheService: this.aiConfigCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
          swaggerService: this.swaggerService,
          graphqlService: this.graphqlService,
        });
        break;



      case 'update_task':
        result = await executeUpdateTask(args, context, {
          conversationService: this.conversationService,
        });
        break;
      case 'get_task':
        result = await executeGetTask(args, context, {
          conversationService: this.conversationService,
        });
        break;
      case 'find_records':
        result = await executeDynamicRepository(
          { ...args, operation: 'find' },
          context,
          abortSignal,
          {
            metadataCacheService: this.metadataCacheService,
            queryBuilder: this.queryBuilder,
            tableHandlerService: this.tableHandlerService,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            aiConfigCacheService: this.aiConfigCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
            swaggerService: this.swaggerService,
            graphqlService: this.graphqlService,
          },
        );
        break;
      case 'create_records':
        result = await executeBatchDynamicRepository(
          {
            ...args,
            operation: 'batch_create',
            dataArray: Array.isArray(args.dataArray) ? args.dataArray : [args.data || args.dataArray],
          },
          context,
          abortSignal,
          {
            metadataCacheService: this.metadataCacheService,
            queryBuilder: this.queryBuilder,
            tableHandlerService: this.tableHandlerService,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            aiConfigCacheService: this.aiConfigCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
            swaggerService: this.swaggerService,
            graphqlService: this.graphqlService,
          },
        );
        break;
      case 'update_records':
        result = await executeBatchDynamicRepository(
          {
            ...args,
            operation: 'batch_update',
            updates: Array.isArray(args.updates) ? args.updates : (args.id ? [{ id: args.id, data: args.data }] : []),
          },
          context,
          abortSignal,
          {
            metadataCacheService: this.metadataCacheService,
            queryBuilder: this.queryBuilder,
            tableHandlerService: this.tableHandlerService,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            aiConfigCacheService: this.aiConfigCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
            swaggerService: this.swaggerService,
            graphqlService: this.graphqlService,
          },
        );
        break;
      case 'delete_records':
        result = await executeBatchDynamicRepository(
          {
            ...args,
            operation: 'batch_delete',
            ids: Array.isArray(args.ids) ? args.ids : (args.id ? [args.id] : []),
          },
          context,
          abortSignal,
          {
            metadataCacheService: this.metadataCacheService,
            queryBuilder: this.queryBuilder,
            tableHandlerService: this.tableHandlerService,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            aiConfigCacheService: this.aiConfigCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
            swaggerService: this.swaggerService,
            graphqlService: this.graphqlService,
          },
        );
        break;
      default:
        throw new Error(`Unknown tool: ${name}`);
    }


    return result;
  }
}
