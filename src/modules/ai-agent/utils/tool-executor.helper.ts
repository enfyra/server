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
      dbTypeContent += `**MongoDB Specific Behavior:**
- Primary key field is "_id" (not "id")
- When creating/updating records, use "_id" field for MongoDB
- When querying by ID, use "_id" field
- ObjectId format is used for "_id" values
- Relations and foreign keys still use property names, but the underlying ID field is "_id"
- When passing relation objects, use "{_id: value}" instead of "{id: value}"

**IMPORTANT: Table names do NOT always have "_definition" suffix!**
- Some tables have "_definition" suffix (e.g., "user_definition", "order_definition")
- Some tables do NOT have this suffix (e.g., "order", "product", "customer")
- ALWAYS use get_metadata first to see the actual table names in the system
- NEVER assume a table has "_definition" suffix - check the metadata!`;
    } else {
      dbTypeContent += `**SQL Database (${dbType}):**
- Primary key field is "id"
- When creating/updating records, use "id" field
- When querying by ID, use "id" field
- Standard integer or UUID format for "id" values
- When passing relation objects, use "{id: value}"

**IMPORTANT: Table names do NOT always have "_definition" suffix!**
- Some tables have "_definition" suffix (e.g., "user_definition", "order_definition")
- Some tables do NOT have this suffix (e.g., "order", "product", "customer")
- ALWAYS use get_metadata first to see the actual table names in the system
- NEVER assume a table has "_definition" suffix - check the metadata!`;
    }

    const dbTypeHint = {
      category: 'database_type',
      title: 'Database Type Information',
      content: dbTypeContent,
    };

    const relationContent = `Enfyra handles relations by accepting objects with ${idFieldName} fields.

**CRITICAL: Always use relation propertyName, NEVER use FK column names!**
- Use "mainTable" (propertyName), NOT "mainTableId" (FK column)
- Use "category" (propertyName), NOT "categoryId" (FK column)
- Use "user" (propertyName), NOT "userId" (FK column)
- Check table metadata to find the correct propertyName for each relation

**CRITICAL: Error Handling - YOU MUST FOLLOW THIS!**
- If ANY operation returns an error (error: true), YOU MUST STOP IMMEDIATELY
- DO NOT call any more tools after receiving an error
- DO NOT try to fix the error automatically by calling find/create/update/delete
- DO NOT retry the same operation with different parameters
- IMMEDIATELY report the error message to the user and ask what they want to do
- The only exception: if the error suggests calling get_hint or get_metadata

**CRITICAL: Delete Operation Requirements**
- Delete operation REQUIRES an "id" parameter - you CANNOT use "where" clause
- If you need to delete by name/other field, you MUST first find the record to get its id
- Example: To delete table named "products", first call find with where: {name: {_eq: "products"}}, then use the returned id for delete

**Many-to-One (M2O):**
- Pass object with ${idFieldName}: System extracts ${idFieldName} as foreign key
- Pass number/string: Used directly as foreign key
- Pass null: Sets foreign key to null
- ALWAYS use propertyName, not FK column name
Example: {"mainTable": {"${idFieldName}": 1}} or {"mainTable": 1} (NOT {"mainTableId": 1})
Example: {"category": {"${idFieldName}": 1}} or {"category": 1} (NOT {"categoryId": 1})

**One-to-One (O2O):**
- Object with ${idFieldName}: Links to existing record (sets FK)
- Object without ${idFieldName}: Creates new related entity and links to it (cascade create)
- ALWAYS use propertyName, not FK column name
Example: {"profile": {"${idFieldName}": 5}} or {"profile": {"bio": "Hello"}} (NOT {"profileId": 5})

**Many-to-Many (M2M):**
- Array of objects/ids: Extracts IDs and synchronizes junction table
- Can mix objects with ${idFieldName} and plain IDs
- ALWAYS use propertyName, not FK column name
Example: {"tags": [{"${idFieldName}": 1}, {"${idFieldName}": 2}, 3]} (use propertyName "tags", not junction table columns)

**One-to-Many (O2M):**
- Item with ${idFieldName}: Updates that item's FK to point to parent (UPDATE)
- Item without ${idFieldName}: Creates new item with FK pointing to parent (CREATE)
- Items not in array: Their FK is set to null (removed from relation)
- ALWAYS use propertyName, not FK column name
Example: {"items": [{"${idFieldName}": 10, "quantity": 5}, {"productId": 1, "quantity": 2}]} (use propertyName "items")

**Important:**
- ALWAYS use relation propertyName from table metadata, NEVER use FK column names (like mainTableId, categoryId, userId, etc.)
- Relation property names are automatically transformed to foreign keys by the system
- Cascade operations happen after main record is created/updated
- Always check get_table_details to see the correct propertyName for each relation
- For MongoDB, use "_id" instead of "id" in relation objects`;

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

