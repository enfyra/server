import { BadRequestException, ForbiddenException } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { PolicyService } from '../../../core/policy/policy.service';
import { isPolicyDeny } from '../../../core/policy/policy.types';
import { TableValidationService } from '../services/table-validation.service';
import { TDynamicContext } from '../../../shared/types';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { SettingCacheService } from '../../../infrastructure/cache/services/setting-cache.service';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { FieldPermissionCacheService } from '../../../infrastructure/cache/services/field-permission-cache.service';
import {
  buildRequestedShapeFromQuery,
  sanitizeFieldPermissionsResult,
} from '../../../shared/utils/sanitize-field-permissions.util';
import {
  decideFieldPermission,
  formatFieldPermissionErrorMessage,
} from '../../../shared/utils/field-permission.util';

export class DynamicRepository {
  public context: TDynamicContext;
  private tableName: string;
  private queryEngine: QueryEngine;
  private queryBuilder: QueryBuilderService;
  private tableHandlerService: TableHandlerService;
  private policyService: PolicyService;
  private tableValidationService: TableValidationService;
  private metadataCacheService: MetadataCacheService;
  private settingCacheService: SettingCacheService;
  private eventEmitter: EventEmitter2;
  private fieldPermissionCacheService?: FieldPermissionCacheService;
  private enforceFieldPermission: boolean;
  private tableMetadata: any;

  constructor({
    context,
    tableName,
    queryEngine,
    queryBuilder,
    tableHandlerService,
    policyService,
    tableValidationService,
    metadataCacheService,
    settingCacheService,
    eventEmitter,
    fieldPermissionCacheService,
    enforceFieldPermission,
  }: {
    context: TDynamicContext;
    tableName: string;
    queryEngine: QueryEngine;
    queryBuilder: QueryBuilderService;
    tableHandlerService: TableHandlerService;
    policyService: PolicyService;
    tableValidationService: TableValidationService;
    metadataCacheService: MetadataCacheService;
    settingCacheService: SettingCacheService;
    eventEmitter: EventEmitter2;
    fieldPermissionCacheService?: FieldPermissionCacheService;
    enforceFieldPermission?: boolean;
  }) {
    this.context = context;
    this.tableName = tableName;
    this.queryEngine = queryEngine;
    this.queryBuilder = queryBuilder;
    this.tableHandlerService = tableHandlerService;
    this.policyService = policyService;
    this.tableValidationService = tableValidationService;
    this.metadataCacheService = metadataCacheService;
    this.settingCacheService = settingCacheService;
    this.eventEmitter = eventEmitter;
    this.fieldPermissionCacheService = fieldPermissionCacheService;
    this.enforceFieldPermission = enforceFieldPermission === true;
  }

  async init() {
    this.tableMetadata = await this.metadataCacheService.lookupTableByName(
      this.tableName,
    );
  }

  private async ensureInit() {
    if (!this.tableMetadata) {
      this.tableMetadata = await this.metadataCacheService.lookupTableByName(
        this.tableName,
      );
    }
  }

  private getIdField(): string {
    return this.queryBuilder.isMongoDb() ? '_id' : 'id';
  }

  private getItemId(item: any): any {
    if (item == null) return null;
    if (typeof item === 'string' || typeof item === 'number') return item;
    return item?._id ?? item?.id ?? null;
  }

  private async canonicalizeFieldPermissionDefinition(data: any): Promise<void> {
    if (!data || typeof data !== 'object') return;
    if (this.tableName !== 'field_permission_definition') return;

    const columnId = this.getItemId(data.column);
    const relationId = this.getItemId(data.relation);

    if (columnId != null && relationId != null) {
      return;
    }

    if (columnId != null) {
      const res = await this.queryBuilder.select({
        tableName: 'column_definition',
        fields: ['id', 'table.*'],
        filter: { id: { _eq: columnId } },
        limit: 1,
      } as any);
      const row = res?.data?.[0];
      const tableId = this.getItemId(row?.table);
      if (tableId != null) {
        data.table = { id: tableId };
      }
      return;
    }

    if (relationId != null) {
      const res = await this.queryBuilder.select({
        tableName: 'relation_definition',
        fields: ['id', 'sourceTable.*'],
        filter: { id: { _eq: relationId } },
        limit: 1,
      } as any);
      const row = res?.data?.[0];
      const tableId = this.getItemId(row?.sourceTable);
      if (tableId != null) {
        data.table = { id: tableId };
      }
    }
  }

  private async assertQueryAllowed() {
    if (!this.enforceFieldPermission) return;
    if (!this.fieldPermissionCacheService) return;
    if (this.context?.$user?.isRootAdmin) return;

    const meta = await this.metadataCacheService.lookupTableByName(this.tableName);
    if (!meta) return;

    const policies = await this.fieldPermissionCacheService.getPoliciesFor(
      this.context.$user,
      this.tableName,
      'read',
    );

    const allowedColumns = new Set<string>();
    const allowedRelations = new Set<string>();
    for (const p of policies) {
      for (const c of p.unconditionalAllowedColumns) allowedColumns.add(c);
      for (const r of p.unconditionalAllowedRelations) allowedRelations.add(r);
    }

    const deniedQueryFields: Array<{ type: 'column' | 'relation'; name: string }> = [];

    const checkColumn = (name: string) => {
      const col = meta.columns?.find((c: any) => c.name === name);
      if (!col) return;
      if (col.isPublished !== false) return;
      if (allowedColumns.has(name)) return;
      deniedQueryFields.push({ type: 'column', name });
    };

    const checkRelation = (name: string) => {
      const rel = meta.relations?.find((r: any) => r.propertyName === name);
      if (!rel) return;
      if (rel.isPublished !== false) return;
      if (allowedRelations.has(name)) return;
      deniedQueryFields.push({ type: 'relation', name });
    };

    const filter = this.context.$query?.filter;
    const sort = this.context.$query?.sort;
    const aggregate = this.context.$query?.aggregate;

    const walkFilter = (node: any) => {
      if (!node || typeof node !== 'object') return;
      if (Array.isArray(node)) {
        node.forEach(walkFilter);
        return;
      }
      if (Array.isArray(node._and)) node._and.forEach(walkFilter);
      if (Array.isArray(node._or)) node._or.forEach(walkFilter);
      for (const k of Object.keys(node)) {
        if (k === '_and' || k === '_or' || k === '_not') continue;
        if (k.includes('.')) {
          const [first] = k.split('.');
          if (first) checkRelation(first);
        } else {
          checkColumn(k);
        }
      }
    };

    walkFilter(filter);

    const sortArr = Array.isArray(sort)
      ? sort
      : typeof sort === 'string'
        ? sort.split(',').map((s) => s.trim()).filter(Boolean)
        : [];
    for (const s of sortArr) {
      const clean = s.startsWith('-') ? s.slice(1) : s;
      if (!clean) continue;
      if (clean.includes('.')) {
        const [first] = clean.split('.');
        if (first) checkRelation(first);
      } else {
        checkColumn(clean);
      }
    }

    if (aggregate && typeof aggregate === 'object') {
      for (const k of Object.keys(aggregate)) {
        const val = aggregate[k];
        if (val && typeof val === 'object') {
          for (const colName of Object.keys(val)) checkColumn(colName);
        }
      }
    }

    if (deniedQueryFields.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({
          action: 'filter',
          tableName: this.tableName,
          fields: deniedQueryFields,
        }),
      );
    }
  }

  async find(
    opt: {
      filter?: any;
      where?: any;
      fields?: string | string[];
      limit?: number;
      sort?: string;
      meta?: string | string[];
    } = {},
  ) {
    await this.ensureInit();
    await this.assertQueryAllowed();
    const debugMode =
      this.context.$query?.debugMode === 'true' ||
      this.context.$query?.debugMode === true;
    const filterValue =
      opt?.filter ?? opt?.where ?? this.context.$query?.filter ?? {};
    const result = await this.queryEngine.find({
      tableName: this.tableName,
      fields: opt?.fields || this.context.$query?.fields || '',
      filter: filterValue,
      page: this.context.$query?.page || 1,
      limit:
        opt && 'limit' in opt ? opt.limit : (this.context.$query?.limit ?? 10),
      meta: opt?.meta || this.context.$query?.meta,
      sort: opt?.sort || this.context.$query?.sort || 'id',
      aggregate: this.context.$query?.aggregate || {},
      deep: this.context.$query?.deep || {},
      debugMode: debugMode,
      maxQueryDepth: this.settingCacheService.getMaxQueryDepth(),
    } as any);

    if (!this.enforceFieldPermission || !this.fieldPermissionCacheService) {
      return result;
    }
    if (this.context?.$user?.isRootAdmin) {
      return result;
    }

    const requested = buildRequestedShapeFromQuery({
      fields: opt?.fields || this.context.$query?.fields,
      deep: this.context.$query?.deep,
    });

    const sanitizedData = await sanitizeFieldPermissionsResult({
      value: result?.data ?? [],
      tableName: this.tableName,
      user: this.context.$user,
      action: 'read',
      metadataCacheService: this.metadataCacheService,
      fieldPermissionCacheService: this.fieldPermissionCacheService,
      requested,
    });

    return {
      ...result,
      data: sanitizedData,
    };
  }

  async create(opt: { data: any; fields?: string | string[] }) {
    await this.ensureInit();
    try {
      const { data: body, fields } = opt;
      if (!body || typeof body !== 'object') {
        throw new BadRequestException('data is required and must be an object');
      }

      await this.canonicalizeFieldPermissionDefinition(body);

      if (this.enforceFieldPermission && this.fieldPermissionCacheService) {
        if (this.context?.$user?.isRootAdmin) {
        } else {
        const meta = await this.metadataCacheService.lookupTableByName(this.tableName);
        if (meta) {
          const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
          for (const key of Object.keys(body)) {
            const col = meta.columns?.find((c: any) => c.name === key);
            if (col) {
              const defaultAllowed = col.isPublished !== false;
              const decision = await decideFieldPermission(
                this.fieldPermissionCacheService,
                {
                  user: this.context.$user,
                  tableName: this.tableName,
                  action: 'create',
                  subjectType: 'column',
                  subjectName: key,
                  record: body,
                },
                { defaultAllowed },
              );
              if (!decision.allowed) denied.push({ type: 'column', name: key });
            }
            const rel = meta.relations?.find((r: any) => r.propertyName === key);
            if (rel) {
              const defaultAllowed = rel.isPublished !== false;
              const decision = await decideFieldPermission(
                this.fieldPermissionCacheService,
                {
                  user: this.context.$user,
                  tableName: this.tableName,
                  action: 'create',
                  subjectType: 'relation',
                  subjectName: key,
                  record: body,
                },
                { defaultAllowed },
              );
              if (!decision.allowed) denied.push({ type: 'relation', name: key });
            }
          }
          if (denied.length > 0) {
            throw new ForbiddenException(
              formatFieldPermissionErrorMessage({
                action: 'create',
                tableName: this.tableName,
                fields: denied,
              }),
            );
          }
        }
        }
      }

      await this.tableValidationService.assertTableValid({
        operation: 'create',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });
      const createDecision = await this.policyService.checkMutationSafety({
        operation: 'create',
        tableName: this.tableName,
        data: body,
        existing: null,
        currentUser: this.context.$user,
      });
      if (isPolicyDeny(createDecision)) {
        throw new BadRequestException(createDecision.message);
      }
      if (this.tableName === 'route_definition') {
        this.filterPublishedMethodsToAvailable(body, null);
      }
      if (this.tableName === 'extension_definition' && body.code) {
        const { processExtensionDefinition } =
          await import('../../extension-definition/utils/processor.util');
        const { processedBody } = await processExtensionDefinition(
          body,
          'POST',
        );
        Object.assign(body, processedBody);
      }
      if (this.tableName === 'table_definition') {
        body.isSystem = false;
        const table: any = await this.tableHandlerService.createTable(
          body,
          this.context,
        );
        await this.reload();
        const idValue = table._id || table.id;
        return await this.find({
          where: { [this.getIdField()]: { _eq: idValue } },
          fields,
        });
      }
      if (body.id !== undefined) {
        delete body.id;
      }
      const inserted = await this.queryBuilder.runWithPolicy(
        (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
        () => this.queryBuilder.insertAndGet(this.tableName, body),
      );
      const createdId = inserted.id || inserted._id || body.id;
      try {
        const result = await this.find({
          where: { [this.getIdField()]: { _eq: createdId } },
          fields,
        });
        await this.reload();
        return result;
      } catch (error: any) {
        const errorMessage = error?.message || error?.toString() || '';
        if (
          errorMessage.includes('operator does not exist') ||
          errorMessage.includes('character varying')
        ) {
          await this.reload();
          return {
            data: [inserted],
            count: 1,
          };
        }
        throw error;
      }
    } catch (error: any) {
      if (error instanceof ForbiddenException) {
        throw error;
      }
      if (error.errInfo) {
        const errorMessage = error.errInfo?.details?.details
          ? JSON.stringify(error.errInfo.details.details, null, 2)
          : error.message || 'Document failed validation';
        throw new BadRequestException(errorMessage);
      }
      throw new BadRequestException(
        error.message || 'Document failed validation',
      );
    }
  }

  async update(opt: {
    id: string | number;
    data: any;
    fields?: string | string[];
  }) {
    await this.ensureInit();
    try {
      const { id, data: body, fields } = opt;
      const existsResult = await this.find({
        where: { [this.getIdField()]: { _eq: id } },
      });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);

      await this.canonicalizeFieldPermissionDefinition(body);

      if (this.enforceFieldPermission && this.fieldPermissionCacheService) {
        if (this.context?.$user?.isRootAdmin) {
        } else {
        const meta = await this.metadataCacheService.lookupTableByName(this.tableName);
        if (meta) {
          const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
          for (const key of Object.keys(body || {})) {
            const col = meta.columns?.find((c: any) => c.name === key);
            if (col) {
              const defaultAllowed = col.isPublished !== false;
              const decision = await decideFieldPermission(
                this.fieldPermissionCacheService,
                {
                  user: this.context.$user,
                  tableName: this.tableName,
                  action: 'update',
                  subjectType: 'column',
                  subjectName: key,
                  record: exists,
                },
                { defaultAllowed },
              );
              if (!decision.allowed) denied.push({ type: 'column', name: key });
            }
            const rel = meta.relations?.find((r: any) => r.propertyName === key);
            if (rel) {
              const defaultAllowed = rel.isPublished !== false;
              const decision = await decideFieldPermission(
                this.fieldPermissionCacheService,
                {
                  user: this.context.$user,
                  tableName: this.tableName,
                  action: 'update',
                  subjectType: 'relation',
                  subjectName: key,
                  record: exists,
                },
                { defaultAllowed },
              );
              if (!decision.allowed) denied.push({ type: 'relation', name: key });
            }
          }
          if (denied.length > 0) {
            throw new ForbiddenException(
              formatFieldPermissionErrorMessage({
                action: 'update',
                tableName: this.tableName,
                fields: denied,
              }),
            );
          }
        }
        }
      }

      await this.tableValidationService.assertTableValid({
        operation: 'update',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });
      const updateDecision = await this.policyService.checkMutationSafety({
        operation: 'update',
        tableName: this.tableName,
        data: body,
        existing: exists,
        currentUser: this.context.$user,
      });
      if (isPolicyDeny(updateDecision)) {
        throw new BadRequestException(updateDecision.message);
      }
      if (this.tableName === 'route_definition' && body.publishedMethods) {
        this.filterPublishedMethodsToAvailable(body, exists);
      }
      if (this.tableName === 'extension_definition' && body.code) {
        const { processExtensionDefinition } =
          await import('../../extension-definition/utils/processor.util');
        const { processedBody } = await processExtensionDefinition(
          body,
          'PATCH',
        );
        Object.assign(body, processedBody);
      }
      if (this.tableName === 'table_definition') {
        const table: any = await this.tableHandlerService.updateTable(
          id,
          body,
          this.context,
        );
        if (table?._preview) {
          return { data: [table] };
        }
        const tableId = table._id || table.id;
        await this.reload();
        return this.find({
          where: { [this.getIdField()]: { _eq: tableId } },
          fields,
        });
      }
      await this.queryBuilder.runWithPolicy(
        (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
        () => this.queryBuilder.updateById(this.tableName, id, body),
      );
      const result = await this.find({
        where: { [this.getIdField()]: { _eq: id } },
        fields,
      });
      await this.reload();
      return result;
    } catch (error: any) {
      if (error instanceof ForbiddenException) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  async delete(opt: { id: string | number }) {
    await this.ensureInit();
    try {
      const { id } = opt;
      const idField = this.getIdField();
      const existsResult = await this.find({
        where: { [idField]: { _eq: id } },
      });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      await this.tableValidationService.assertTableValid({
        operation: 'delete',
        tableName: this.tableName,
        tableMetadata: this.tableMetadata,
      });
      const deleteDecision = await this.policyService.checkMutationSafety({
        operation: 'delete',
        tableName: this.tableName,
        data: {},
        existing: exists,
        currentUser: this.context.$user,
      });
      if (isPolicyDeny(deleteDecision)) {
        throw new BadRequestException(deleteDecision.message);
      }
      if (this.tableName === 'table_definition') {
        await this.tableHandlerService.delete(id, this.context);
        await this.reload();
        return { message: 'Success', statusCode: 200 };
      }
      await this.queryBuilder.runWithPolicy(
        (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
        () => this.queryBuilder.deleteById(this.tableName, id),
      );
      await this.reload();
      return { message: 'Delete successfully!', statusCode: 200 };
    } catch (error) {
      throw new BadRequestException(error.message);
    }
  }

  private toMethodIds(arr: any[]): number[] {
    if (!Array.isArray(arr)) return [];
    return arr
      .map((item) =>
        item && typeof item === 'object' && 'id' in item ? item.id : item,
      )
      .filter((id): id is number => id != null && typeof id === 'number');
  }

  private filterPublishedMethodsToAvailable(body: any, existing: any): void {
    const availableIds = new Set<number>(
      body.availableMethods
        ? this.toMethodIds(
            Array.isArray(body.availableMethods) ? body.availableMethods : [],
          )
        : existing?.availableMethods
          ? this.toMethodIds(
              Array.isArray(existing.availableMethods)
                ? existing.availableMethods
                : [],
            )
          : [],
    );
    if (availableIds.size === 0) {
      body.publishedMethods = [];
      return;
    }
    const published = Array.isArray(body.publishedMethods)
      ? body.publishedMethods
      : [];
    const filtered = published.filter((item: any) => {
      const id =
        item && typeof item === 'object' && 'id' in item ? item.id : item;
      return id != null && availableIds.has(Number(id));
    });
    body.publishedMethods = filtered;
  }

  private async cascadePolicyCheck(
    tableName: string,
    operation: 'create' | 'update' | 'delete',
    data: any,
  ): Promise<void> {
    const decision = await this.policyService.checkMutationSafety({
      operation,
      tableName,
      data,
      existing: null,
      currentUser: this.context.$user,
    });
    if (isPolicyDeny(decision)) {
      throw new BadRequestException(decision.message);
    }
  }

  private async reload() {
    await new Promise((resolve) => setTimeout(resolve, 750));
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, {
      tableName: this.tableName,
      action: 'reload',
      timestamp: Date.now(),
    });
  }
}
