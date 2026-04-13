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
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
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
    return this.queryBuilder.getPkField();
  }

  private getItemId(item: any): any {
    if (item == null) return null;
    if (typeof item === 'string' || typeof item === 'number') return item;
    return item?._id ?? item?.id ?? null;
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

  private async hasConditionalRulesForField(
    tableName: string,
    action: 'read' | 'create' | 'update',
    subjectType: 'column' | 'relation',
    subjectName: string,
  ): Promise<boolean> {
    if (!this.fieldPermissionCacheService) return false;
    const policies = await this.fieldPermissionCacheService.getPoliciesFor(
      this.context.$user,
      tableName,
      action,
    );
    for (const p of policies) {
      for (const r of p.rules) {
        if (r.condition == null) continue;
        if (r.tableName !== tableName || r.action !== action) continue;
        if (subjectType === 'column' && r.columnName === subjectName) return true;
        if (subjectType === 'relation' && r.relationPropertyName === subjectName) return true;
      }
    }
    return false;
  }

  private async stripDeniedFields(
    tableName: string,
    fields: string | string[] | undefined,
    deep: Record<string, any> | undefined,
  ): Promise<{ fields: string | string[] | undefined; deep: Record<string, any> | undefined; needsPostSql: boolean }> {
    if (!this.enforceFieldPermission || !this.fieldPermissionCacheService) {
      return { fields, deep, needsPostSql: false };
    }


    const meta = await this.metadataCacheService.lookupTableByName(tableName);
    if (!meta) return { fields, deep, needsPostSql: false };

    let hasConditionalPending = false;

    const columnSet = new Set<string>(
      (meta.columns || []).map((c: any) => c.name as string),
    );
    const relationSet = new Set<string>(
      (meta.relations || []).map((r: any) => r.propertyName as string),
    );

    const isWildcard =
      !fields ||
      (typeof fields === 'string' && (fields === '' || fields === '*')) ||
      (Array.isArray(fields) && (fields.length === 0 || fields.includes('*')));

    let fieldsArr: string[];
    if (isWildcard) {
      fieldsArr = [...columnSet, ...relationSet];
    } else {
      fieldsArr =
        typeof fields === 'string'
          ? fields.split(',').map((s) => s.trim()).filter(Boolean)
          : [...(fields as string[])];
    }

    const columnsToCheck = new Set<string>();
    const relationsToCheck = new Set<string>();
    for (const f of fieldsArr) {
      const first = f.split('.')[0];
      if (first && columnSet.has(first)) columnsToCheck.add(first);
      if (first && relationSet.has(first)) relationsToCheck.add(first);
    }
    for (const key of Object.keys(deep || {})) {
      if (relationSet.has(key)) relationsToCheck.add(key);
    }

    const deniedColumns = new Set<string>();
    for (const colName of columnsToCheck) {
      const col = (meta.columns || []).find((c: any) => c.name === colName);
      if (col?.isPrimary) continue;
      const defaultAllowed = col?.isPublished !== false;
      const decision = await decideFieldPermission(
        this.fieldPermissionCacheService,
        {
          user: this.context.$user,
          tableName,
          action: 'read',
          subjectType: 'column',
          subjectName: colName,
          record: null,
        },
        { defaultAllowed },
      );
      if (!decision.allowed) {
        if (defaultAllowed) {
          deniedColumns.add(colName);
        } else {
          const hasConditional = await this.hasConditionalRulesForField(tableName, 'read', 'column', colName);
          if (!hasConditional) deniedColumns.add(colName);
          else hasConditionalPending = true;
        }
      }
    }

    const deniedRelations = new Set<string>();
    for (const relName of relationsToCheck) {
      const rel = (meta.relations || []).find((r: any) => r.propertyName === relName);
      const defaultAllowed = rel?.isPublished !== false;
      const decision = await decideFieldPermission(
        this.fieldPermissionCacheService,
        {
          user: this.context.$user,
          tableName,
          action: 'read',
          subjectType: 'relation',
          subjectName: relName,
          record: null,
        },
        { defaultAllowed },
      );
      if (!decision.allowed) {
        if (defaultAllowed) {
          deniedRelations.add(relName);
        } else {
          const hasConditional = await this.hasConditionalRulesForField(tableName, 'read', 'relation', relName);
          if (!hasConditional) deniedRelations.add(relName);
          else hasConditionalPending = true;
        }
      }
    }

    const hasDenied = deniedColumns.size > 0 || deniedRelations.size > 0;
    const cleanFieldsArr = hasDenied
      ? fieldsArr.filter((f) => {
          const first = f.split('.')[0];
          return !deniedColumns.has(first) && !deniedRelations.has(first);
        })
      : fieldsArr;

    const cleanFields =
      typeof fields === 'string' || isWildcard
        ? cleanFieldsArr.join(',')
        : cleanFieldsArr;

    let cleanDeep: Record<string, any> | undefined = deep ? { ...deep } : undefined;
    if (cleanDeep) {
      for (const rel of deniedRelations) {
        delete cleanDeep[rel];
      }
      for (const relName of Object.keys(cleanDeep)) {
        const relEntry = cleanDeep[relName];
        if (!relEntry || typeof relEntry !== 'object') continue;
        if (!relEntry.deep && !relEntry.fields) continue;
        const relMeta = (meta.relations || []).find(
          (r: any) => r.propertyName === relName,
        );
        const targetTable = relMeta?.targetTable || relMeta?.targetTableName;
        if (!targetTable) continue;
        const nested = await this.stripDeniedFields(
          targetTable,
          relEntry.fields,
          relEntry.deep,
        );
        if (nested.needsPostSql) hasConditionalPending = true;
        cleanDeep[relName] = {
          ...relEntry,
          ...(nested.fields !== relEntry.fields ? { fields: nested.fields } : {}),
          ...(nested.deep !== relEntry.deep ? { deep: nested.deep } : {}),
        };
      }
    }

    return { fields: cleanFields, deep: cleanDeep, needsPostSql: hasConditionalPending };
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

    const rawFields = opt?.fields || this.context.$query?.fields;
    const rawDeep: Record<string, any> = this.context.$query?.deep || {};
    const { fields: cleanFields, deep: cleanDeep, needsPostSql } = await this.stripDeniedFields(
      this.tableName,
      rawFields,
      rawDeep,
    );

    const debugMode =
      this.context.$query?.debugMode === 'true' ||
      this.context.$query?.debugMode === true;
    const filterValue =
      opt?.filter ?? opt?.where ?? this.context.$query?.filter ?? {};
    if (this.tableName === 'table_definition') {
    }
    const result = await this.queryEngine.find({
      table: this.tableName,
      fields: cleanFields || '',
      filter: filterValue,
      page: this.context.$query?.page || 1,
      limit:
        opt && 'limit' in opt ? opt.limit : (this.context.$query?.limit ?? 10),
      meta: opt?.meta || this.context.$query?.meta,
      sort: opt?.sort || this.context.$query?.sort || this.getIdField(),
      aggregate: this.context.$query?.aggregate || {},
      deep: cleanDeep || {},
      debugMode: debugMode,
      maxQueryDepth: this.settingCacheService.getMaxQueryDepth(),
    } as any);

    if (!needsPostSql) {
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
        const idValue = table._id || table.id;
        await this.reload({
          ids: [idValue],
          affectedTables: table.affectedTables,
        });
        return await this.find({
          where: { [this.getIdField()]: { _eq: idValue } },
          fields,
        });
      }
      if (body.id !== undefined) {
        delete body.id;
      }
      if (body._id !== undefined) {
        delete body._id;
      }
      const inserted = await this.wrapWithFieldPermissionCheck(() =>
        this.queryBuilder.runWithPolicy(
          (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
          () => this.queryBuilder.insert(this.tableName, body),
        ),
      );
      const createdId = inserted.id || inserted._id || body.id;
      try {
        const result = await this.find({
          where: { [this.getIdField()]: { _eq: createdId } },
          fields,
        });
        await this.reload({ ids: [createdId] });
        return result;
      } catch (error: any) {
        const errorMessage = error?.message || error?.toString() || '';
        if (
          errorMessage.includes('operator does not exist') ||
          errorMessage.includes('character varying')
        ) {
          await this.reload({ ids: [createdId] });
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
        await this.reload({
          ids: [tableId],
          affectedTables: table.affectedTables,
        });
        return this.find({
          where: { [this.getIdField()]: { _eq: tableId } },
          fields,
        });
      }
      await this.wrapWithFieldPermissionCheck(() =>
        this.queryBuilder.runWithPolicy(
          (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
          () => this.queryBuilder.update(this.tableName, id, body),
        ),
      );
      const result = await this.find({
        where: { [this.getIdField()]: { _eq: id } },
        fields,
      });
      await this.reload({ ids: [id] });
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
        const deleted: any = await this.tableHandlerService.delete(id, this.context);
        await this.reload({
          ids: [id],
          affectedTables: deleted?.affectedTables,
        });
        return { message: 'Success', statusCode: 200 };
      }
      await this.queryBuilder.runWithPolicy(
        (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
        () => this.queryBuilder.delete(this.tableName, id),
      );
      await this.reload({ ids: [id] });
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

  private async cascadeFieldPermissionCheck(
    tableName: string,
    action: 'create' | 'update',
    data: any,
  ): Promise<void> {
    if (!this.enforceFieldPermission || !this.fieldPermissionCacheService) return;
    const meta = await this.metadataCacheService.lookupTableByName(tableName);
    if (!meta) return;
    const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
    for (const key of Object.keys(data || {})) {
      const col = meta.columns?.find((c: any) => c.name === key);
      if (col) {
        const decision = await decideFieldPermission(
          this.fieldPermissionCacheService,
          {
            user: this.context.$user,
            tableName,
            action,
            subjectType: 'column',
            subjectName: key,
            record: data,
          },
          { defaultAllowed: col.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'column', name: key });
      }
      const rel = meta.relations?.find((r: any) => r.propertyName === key);
      if (rel) {
        const decision = await decideFieldPermission(
          this.fieldPermissionCacheService,
          {
            user: this.context.$user,
            tableName,
            action,
            subjectType: 'relation',
            subjectName: key,
            record: data,
          },
          { defaultAllowed: rel.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'relation', name: key });
      }
    }
    if (denied.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({ action, tableName, fields: denied }),
      );
    }
  }

  private wrapWithFieldPermissionCheck<T>(
    callback: () => Promise<T>,
  ): Promise<T> {
    if (
      !this.enforceFieldPermission ||
      !this.fieldPermissionCacheService ||
      this.context?.$user?.isRootAdmin
    ) {
      return callback();
    }
    return this.queryBuilder.runWithFieldPermissionCheck(
      (tbl, action, d) => this.cascadeFieldPermissionCheck(tbl, action, d),
      callback,
    );
  }

  private async reload(opts?: {
    ids?: (string | number)[];
    affectedTables?: string[];
  }) {
    const payload: TCacheInvalidationPayload = {
      table: this.tableName,
      action: 'reload',
      timestamp: Date.now(),
      scope: opts?.ids?.length ? 'partial' : 'full',
      ids: opts?.ids,
      affectedTables: opts?.affectedTables,
    };
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, payload);
  }
}
