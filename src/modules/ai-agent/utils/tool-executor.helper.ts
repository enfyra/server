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
import { executeListTables } from './executors/list-tables.executor';
import { executeGetTableDetails } from './executors/get-table-details.executor';
import { executeGetFields } from './executors/get-fields.executor';
import { executeGetHint } from './executors/get-hint.executor';
import { executeCreateTable } from './executors/create-table.executor';
import { executeUpdateTable } from './executors/update-table.executor';
import { executeDeleteTable } from './executors/delete-table.executor';
import { executeUpdateTask } from './executors/update-task.executor';
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
    } catch (e) {
      throw new Error(`Invalid tool arguments: ${argsStr}`);
    }

    switch (name) {
      case 'list_tables':
        return await executeListTables({
          metadataCacheService: this.metadataCacheService,
        });
      case 'get_table_details':
        return await executeGetTableDetails(args, context, {
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
      case 'get_fields':
        return await executeGetFields(args, {
          metadataCacheService: this.metadataCacheService,
        });
      case 'get_hint':
        return await executeGetHint(args, context, {
          queryBuilder: this.queryBuilder,
        });
      case 'create_table':
        return await executeCreateTable(args, context, abortSignal, {
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
      case 'update_table':
        return await executeUpdateTable(args, context, abortSignal, {
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
      case 'delete_table':
        return await executeDeleteTable(args, context, abortSignal, {
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
      case 'update_task':
        return await executeUpdateTask(args, context, {
          conversationService: this.conversationService,
        });
      case 'dynamic_repository':
        return await executeDynamicRepository(args, context, abortSignal, {
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
      case 'batch_dynamic_repository':
        return await executeBatchDynamicRepository(args, context, abortSignal, {
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
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }
}
