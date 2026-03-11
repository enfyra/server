import { BadRequestException } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { SystemProtectionService } from '../services/system-protection.service';
import { TableValidationService } from '../services/table-validation.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';

export class DynamicRepository {
  public context: TDynamicContext;
  private tableName: string;
  private queryEngine: QueryEngine;
  private queryBuilder: QueryBuilderService;
  private tableHandlerService: TableHandlerService;
  private systemProtectionService: SystemProtectionService;
  private tableValidationService: TableValidationService;
  private metadataCacheService: MetadataCacheService;
  private eventEmitter: EventEmitter2;
  private tableMetadata: any;

  constructor({
    context,
    tableName,
    queryEngine,
    queryBuilder,
    tableHandlerService,
    systemProtectionService,
    tableValidationService,
    metadataCacheService,
    eventEmitter,
  }: {
    context: TDynamicContext;
    tableName: string;
    queryEngine: QueryEngine;
    queryBuilder: QueryBuilderService;
    tableHandlerService: TableHandlerService;
    systemProtectionService: SystemProtectionService;
    tableValidationService: TableValidationService;
    metadataCacheService: MetadataCacheService;
    eventEmitter: EventEmitter2;
  }) {
    this.context = context;
    this.tableName = tableName;
    this.queryEngine = queryEngine;
    this.queryBuilder = queryBuilder;
    this.tableHandlerService = tableHandlerService;
    this.systemProtectionService = systemProtectionService;
    this.tableValidationService = tableValidationService;
    this.metadataCacheService = metadataCacheService;
    this.eventEmitter = eventEmitter;
  }

  async init() {
    this.tableMetadata = await this.metadataCacheService.lookupTableByName(this.tableName);
  }

  private getIdField(): string {
    return this.queryBuilder.isMongoDb() ? '_id' : 'id';
  }

  async find(opt: { filter?: any; where?: any; fields?: string | string[]; limit?: number; sort?: string; meta?: string | string[] } = {}) {
    const debugMode = this.context.$query?.debugMode === 'true' || this.context.$query?.debugMode === true;
    const filterValue = opt?.filter ?? opt?.where ?? this.context.$query?.filter ?? {};
    return await this.queryEngine.find({
      tableName: this.tableName,
      fields: opt?.fields || this.context.$query?.fields || '',
      filter: filterValue,
      page: this.context.$query?.page || 1,
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
      if (this.tableName === 'route_definition') {
        this.filterPublishedMethodsToAvailable(body, null);
      }
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
    } catch (error: any) {
      if (error.errInfo) {
        const errorMessage = error.errInfo?.details?.details
          ? JSON.stringify(error.errInfo.details.details, null, 2)
          : error.message || 'Document failed validation';
        throw new BadRequestException(errorMessage);
      }
      throw new BadRequestException(error.message || 'Document failed validation');
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
      if (this.tableName === 'route_definition' && body.publishedMethods) {
        this.filterPublishedMethodsToAvailable(body, exists);
      }
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
      const idField = this.getIdField();
      const existsResult = await this.find({ where: { [idField]: { _eq: id } } });
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

  private toMethodIds(arr: any[]): number[] {
    if (!Array.isArray(arr)) return [];
    return arr
      .map((item) => (item && typeof item === 'object' && 'id' in item ? item.id : item))
      .filter((id): id is number => id != null && typeof id === 'number');
  }

  private filterPublishedMethodsToAvailable(body: any, existing: any): void {
    const availableIds = new Set<number>(
      body.availableMethods
        ? this.toMethodIds(Array.isArray(body.availableMethods) ? body.availableMethods : [])
        : existing?.availableMethods
          ? this.toMethodIds(Array.isArray(existing.availableMethods) ? existing.availableMethods : [])
          : [],
    );
    if (availableIds.size === 0) {
      body.publishedMethods = [];
      return;
    }
    const published = Array.isArray(body.publishedMethods) ? body.publishedMethods : [];
    const filtered = published.filter((item: any) => {
      const id = item && typeof item === 'object' && 'id' in item ? item.id : item;
      return id != null && availableIds.has(Number(id));
    });
    body.publishedMethods = filtered;
  }

  private async reload() {
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, {
      tableName: this.tableName,
      action: 'reload',
      timestamp: Date.now(),
    });
  }
}
