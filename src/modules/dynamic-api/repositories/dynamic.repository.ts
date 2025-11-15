import { BadRequestException } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../../../infrastructure/cache/services/ai-config-cache.service';
import { SystemProtectionService } from '../services/system-protection.service';
import { TableValidationService } from '../services/table-validation.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { BootstrapScriptService } from '../../../core/bootstrap/services/bootstrap-script.service';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY } from '../../../shared/utils/constant';
import { GraphqlService } from 'src/modules/graphql/services/graphql.service';
import { SwaggerService } from 'src/infrastructure/swagger/services/swagger.service';

export class DynamicRepository {
  private context: TDynamicContext;
  private tableName: string;
  private queryEngine: QueryEngine;
  private queryBuilder: QueryBuilderService;
  private tableHandlerService: TableHandlerService;
  private routeCacheService: RouteCacheService;
  private storageConfigCacheService?: StorageConfigCacheService;
  private aiConfigCacheService?: AiConfigCacheService;
  private systemProtectionService: SystemProtectionService;
  private tableValidationService: TableValidationService;
  private bootstrapScriptService?: BootstrapScriptService;
  private redisPubSubService?: RedisPubSubService;
  private metadataCacheService: MetadataCacheService;
  private graphqlService: GraphqlService;
  private swaggerService: SwaggerService;
  private tableMetadata: any;

  constructor({
    context,
    tableName,
    queryEngine,
    queryBuilder,
    tableHandlerService,
    routeCacheService,
    storageConfigCacheService,
    aiConfigCacheService,
    systemProtectionService,
    tableValidationService,
    bootstrapScriptService,
    redisPubSubService,
    metadataCacheService,
    swaggerService,
    graphqlService
  }: {
    context: TDynamicContext;
    tableName: string;
    queryEngine: QueryEngine;
    queryBuilder: QueryBuilderService;
    tableHandlerService: TableHandlerService;
    routeCacheService: RouteCacheService;
    storageConfigCacheService?: StorageConfigCacheService;
    aiConfigCacheService?: AiConfigCacheService;
    systemProtectionService: SystemProtectionService;
    tableValidationService: TableValidationService;
    bootstrapScriptService?: BootstrapScriptService;
    redisPubSubService?: RedisPubSubService;
    metadataCacheService: MetadataCacheService;
    swaggerService?: SwaggerService;
    graphqlService?: GraphqlService;
  }) {
    this.context = context;
    this.tableName = tableName;
    this.queryEngine = queryEngine;
    this.queryBuilder = queryBuilder;
    this.tableHandlerService = tableHandlerService;
    this.routeCacheService = routeCacheService;
    this.storageConfigCacheService = storageConfigCacheService;
    this.aiConfigCacheService = aiConfigCacheService;
    this.systemProtectionService = systemProtectionService;
    this.tableValidationService = tableValidationService;
    this.bootstrapScriptService = bootstrapScriptService;
    this.redisPubSubService = redisPubSubService;
    this.metadataCacheService = metadataCacheService;
    this.swaggerService = swaggerService;
    this.graphqlService = graphqlService;
  }

  async init() {
    this.tableMetadata = await this.metadataCacheService.lookupTableByName(this.tableName);
  }

  private getIdField(): string {
    return this.queryBuilder.isMongoDb() ? '_id' : 'id';
  }

  async find(opt: { where?: any; fields?: string | string[]; limit?: number; sort?: string; meta?: string | string[] }) {
    const debugMode = this.context.$query?.debugMode === 'true' || this.context.$query?.debugMode === true;

    return await this.queryEngine.find({
      tableName: this.tableName,
      fields: opt?.fields || this.context.$query?.fields || '',
      filter: opt?.where || this.context.$query?.filter || {},
      page: this.context.$query?.page || 1,
      // If opt.limit is provided (including 0), prefer it. Otherwise fall back to context or default.
      limit: (opt && 'limit' in opt ? opt.limit : (this.context.$query?.limit ?? 10)),
      meta: opt?.meta || this.context.$query?.meta,
      sort: (opt?.sort || this.context.$query?.sort || 'id'),
      aggregate: this.context.$query?.aggregate || {},
      deep: this.context.$query?.deep || {},
      debugMode: debugMode,
    });
  }

  async create(opt: { data: any; fields?: string | string[] }) {
    try {
      const { data: body, fields } = opt;
      
      if (!body || typeof body !== 'object') {
        throw new BadRequestException('data is required and must be an object');
      }
      
      await this.tableValidationService.assertTableValid({
        operation: 'create',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });

      await this.systemProtectionService.assertSystemSafe({
        operation: 'create',
        tableName: this.tableName,
        data: body,
        existing: null,
        currentUser: this.context.$user,
      });

      if (this.tableName === 'table_definition') {
        body.isSystem = false;
        const table: any = await this.tableHandlerService.createTable(body);
        await this.reload();
        const idValue = table._id || table.id;
        return await this.find({ where: { [this.getIdField()]: { _eq: idValue } }, fields });
      }

      if (body.id !== undefined) {
        delete body.id;
      }

      const inserted = await this.queryBuilder.insertAndGet(this.tableName, body);
      const createdId = inserted.id || inserted._id || body.id;

      try {
        const result = await this.find({ where: { [this.getIdField()]: { _eq: createdId } }, fields });
        await this.reload();
        return result;
      } catch (error: any) {
        // If query fails (e.g., type mismatch), return the inserted data directly
        const errorMessage = error?.message || error?.toString() || '';
        if (errorMessage.includes('operator does not exist') || errorMessage.includes('character varying')) {
          await this.reload();
          return {
            data: [inserted],
            count: 1,
          };
        }
        throw error;
      }
    } catch (error) {
      throw new BadRequestException(error.message);
    }
  }

  async update(opt: { id: string | number; data: any; fields?: string | string[] }) {
    try {
      const { id, data: body, fields } = opt;
      
      const existsResult = await this.find({ where: { [this.getIdField()]: { _eq: id } } });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);

      await this.tableValidationService.assertTableValid({
        operation: 'update',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });

      await this.systemProtectionService.assertSystemSafe({
        operation: 'update',
        tableName: this.tableName,
        data: body,
        existing: exists,
        currentUser: this.context.$user,
      });

      if (this.tableName === 'table_definition') {
        const table: any = await this.tableHandlerService.updateTable(id, body);
        const tableId = table._id || table.id;
        await this.reload();
        return this.find({ where: { [this.getIdField()]: { _eq: tableId } }, fields });
      }

      await this.queryBuilder.updateById(this.tableName, id, body);

      const result = await this.find({ where: { [this.getIdField()]: { _eq: id } }, fields });
      await this.reload();
      return result;
    } catch (error) {
      throw new BadRequestException(error.message);
    }
  }

  async delete(opt: { id: string | number }) {
    try {
      const { id } = opt;
      
      const existsResult = await this.find({ where: { id: { _eq: id } } });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);

      await this.tableValidationService.assertTableValid({
        operation: 'delete',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });

      await this.systemProtectionService.assertSystemSafe({
        operation: 'delete',
        tableName: this.tableName,
        data: {},
        existing: exists,
        currentUser: this.context.$user,
      });

      if (this.tableName === 'table_definition') {
        await this.tableHandlerService.delete(id);
        await this.reload();
        return { message: 'Success', statusCode: 200 };
      }

      await this.queryBuilder.deleteById(this.tableName, id);

      await this.reload();
      return { message: 'Delete successfully!', statusCode: 200 };
    } catch (error) {
      throw new BadRequestException(error.message);
    }
  }

  private async reload() {
    if (
      [
        'table_definition',
        'column_definition',
        'relation_definition',
      ].includes(this.tableName)
    ) {
      await this.metadataCacheService.reload();
    }

    if (
      [
        'route_definition',
        'hook_definition',
        'route_handler_definition',
        'route_permission_definition',
        'role_definition',
        'table_definition',
        'method_definition',
      ].includes(this.tableName)
    ) {
      await this.routeCacheService.reload();
      await this.graphqlService?.reloadSchema();
      await this.swaggerService?.reloadSwagger();
    }

    if (this.tableName === 'bootstrap_script_definition' && this.bootstrapScriptService) {
      if (this.redisPubSubService) {
        await this.redisPubSubService.publish(BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY, {
          timestamp: Date.now(),
          tableName: this.tableName,
          action: 'reload',
          message: 'Bootstrap script definition changed - triggering reload'
        });
      }
      
      await this.bootstrapScriptService.reloadBootstrapScripts();
    }

    if (this.tableName === 'storage_config_definition' && this.storageConfigCacheService) {
      await this.storageConfigCacheService.reload();
    }

    if (this.tableName === 'ai_config_definition' && this.aiConfigCacheService) {
      await this.aiConfigCacheService.reload();
    }
  }
}
