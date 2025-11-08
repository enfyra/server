import { BadRequestException } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { SystemProtectionService } from '../services/system-protection.service';
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
  private systemProtectionService: SystemProtectionService;
  private bootstrapScriptService?: BootstrapScriptService;
  private redisPubSubService?: RedisPubSubService;
  private metadataCacheService: MetadataCacheService;
  private graphqlService: GraphqlService;
  private swaggerService: SwaggerService;

  constructor({
    context,
    tableName,
    queryEngine,
    queryBuilder,
    tableHandlerService,
    routeCacheService,
    storageConfigCacheService,
    systemProtectionService,
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
    systemProtectionService: SystemProtectionService;
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
    this.systemProtectionService = systemProtectionService;
    this.bootstrapScriptService = bootstrapScriptService;
    this.redisPubSubService = redisPubSubService;
    this.metadataCacheService = metadataCacheService;
    this.swaggerService = swaggerService;
    this.graphqlService = graphqlService;
  }

  async init() {
  }

  private getIdField(): string {
    return this.queryBuilder.isMongoDb() ? '_id' : 'id';
  }

  async find(opt: { where?: any; fields?: string | string[] }) {
    const debugMode = this.context.$query?.debugMode === 'true' || this.context.$query?.debugMode === true;

    return await this.queryEngine.find({
      tableName: this.tableName,
      fields: opt?.fields || this.context.$query?.fields || '',
      filter: opt?.where || this.context.$query?.filter || {},
      page: this.context.$query?.page || 1,
      limit: this.context.$query?.limit || 10,
      meta: this.context.$query?.meta,
      sort: this.context.$query?.sort || 'id',
      aggregate: this.context.$query?.aggregate || {},
      deep: this.context.$query?.deep || {},
      debugMode: debugMode,
    });
  }

  async create(body: any) {
    try {
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
        return await this.find({ where: { [this.getIdField()]: { _eq: idValue } } });
      }

      const metadata = await this.metadataCacheService.lookupTableByName(this.tableName);

      if (!this.queryBuilder.isMongoDb() && metadata?.columns?.some((c: any) => c.isPrimary && c.type === 'uuid')) {
        body.id = body.id || randomUUID();
      }

      const inserted = await this.queryBuilder.insertAndGet(this.tableName, body);
      const createdId = inserted.id || inserted._id || body.id;

      const result = await this.find({ where: { [this.getIdField()]: { _eq: createdId } } });
      await this.reload();
      return result;
    } catch (error) {
      console.error('Error in dynamic repo [create]:', error);
      throw new BadRequestException(error.message);
    }
  }

  async update(id: string | number, body: any) {
    try {
      const existsResult = await this.find({ where: { [this.getIdField()]: { _eq: id } } });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);

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
        return this.find({ where: { [this.getIdField()]: { _eq: tableId } } });
      }

      await this.queryBuilder.updateById(this.tableName, id, body);

      const result = await this.find({ where: { [this.getIdField()]: { _eq: id } } });
      await this.reload();
      return result;
    } catch (error) {
      console.error('Error in dynamic repo [update]:', error);
      throw new BadRequestException(error.message);
    }
  }

  async delete(id: string | number) {
    try {
      const existsResult = await this.find({ where: { id: { _eq: id } } });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);

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
      console.error('Error in dynamic repo [delete]:', error);
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
  }
}
