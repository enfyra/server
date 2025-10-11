import { BadRequestException } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { KnexService } from '../../../infrastructure/knex/knex.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { SystemProtectionService } from '../services/system-protection.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { BootstrapScriptService } from '../../../core/bootstrap/services/bootstrap-script.service';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY } from '../../../shared/utils/constant';

export class DynamicRepository {
  private context: TDynamicContext;
  private tableName: string;
  private queryEngine: QueryEngine;
  private knexService: KnexService;
  private tableHandlerService: TableHandlerService;
  private routeCacheService: RouteCacheService;
  private systemProtectionService: SystemProtectionService;
  private bootstrapScriptService?: BootstrapScriptService;
  private redisPubSubService?: RedisPubSubService;
  private metadataCacheService: MetadataCacheService;

  constructor({
    context,
    tableName,
    queryEngine,
    knexService,
    tableHandlerService,
    routeCacheService,
    systemProtectionService,
    bootstrapScriptService,
    redisPubSubService,
    metadataCacheService,
  }: {
    context: TDynamicContext;
    tableName: string;
    queryEngine: QueryEngine;
    knexService: KnexService;
    tableHandlerService: TableHandlerService;
    routeCacheService: RouteCacheService;
    systemProtectionService: SystemProtectionService;
    bootstrapScriptService?: BootstrapScriptService;
    redisPubSubService?: RedisPubSubService;
    metadataCacheService: MetadataCacheService;
  }) {
    this.context = context;
    this.tableName = tableName;
    this.queryEngine = queryEngine;
    this.knexService = knexService;
    this.tableHandlerService = tableHandlerService;
    this.routeCacheService = routeCacheService;
    this.systemProtectionService = systemProtectionService;
    this.bootstrapScriptService = bootstrapScriptService;
    this.redisPubSubService = redisPubSubService;
    this.metadataCacheService = metadataCacheService;
  }

  async init() {
    // No need to initialize repo with Knex - direct queries
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
        return await this.find({ where: { id: { _eq: table.id } } });
      }

      const knex = this.knexService.getKnex();
      const metadata = await this.metadataCacheService.getTableMetadata(this.tableName);
      
      // Generate UUID for primary key if needed
      if (metadata?.columns?.some((c: any) => c.isPrimary && c.type === 'uuid')) {
        body.id = body.id || randomUUID();
      }

      const [id] = await knex(this.tableName).insert(body);
      const createdId = body.id || id;
      
      const result = await this.find({ where: { id: { _eq: createdId } } });
      await this.reload();
      return result;
    } catch (error) {
      console.error('❌ Error in dynamic repo [create]:', error);
      throw new BadRequestException(error.message);
    }
  }

  async update(id: string | number, body: any) {
    try {
      const existsResult = await this.find({ where: { id: { _eq: id } } });
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
        const table: any = await this.tableHandlerService.updateTable(
          +id,
          body,
        );
        return this.find({ where: { id: { _eq: table.id } } });
      }

      const knex = this.knexService.getKnex();
      await knex(this.tableName).where('id', id).update(body);

      const result = await this.find({ where: { id: { _eq: id } } });
      await this.reload();
      return result;
    } catch (error) {
      console.error('❌ Error in dynamic repo [update]:', error);
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
        await this.tableHandlerService.delete(+id);
        return { message: 'Success', statusCode: 200 };
      }

      const knex = this.knexService.getKnex();
      await knex(this.tableName).where('id', id).delete();

      await this.reload();
      return { message: 'Delete successfully!', statusCode: 200 };
    } catch (error) {
      console.error('❌ Error in dynamic repo [delete]:', error);
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
      await this.metadataCacheService.reloadMetadataCache();
    }

    if (
      [
        'route_definition',
        'hook_definition',
        'route_handler_definition',
        'route_permission_definition',
        'role_definition',
        'table_definition',
      ].includes(this.tableName)
    ) {
      await this.routeCacheService.reloadRouteCache();
    }

    // Reload bootstrap scripts when bootstrap_script_definition changes
    if (this.tableName === 'bootstrap_script_definition' && this.bootstrapScriptService) {
      // Publish event to notify all instances about bootstrap script change
      if (this.redisPubSubService) {
        await this.redisPubSubService.publish(BOOTSTRAP_SCRIPT_RELOAD_EVENT_KEY, {
          timestamp: Date.now(),
          tableName: this.tableName,
          action: 'reload',
          message: 'Bootstrap script definition changed - triggering reload'
        });
      }
      
      // Reload on current instance
      await this.bootstrapScriptService.reloadBootstrapScripts();
    }
  }
}
