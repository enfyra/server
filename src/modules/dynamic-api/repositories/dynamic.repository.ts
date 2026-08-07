import {
  BadRequestException,
  ConflictException,
  ForbiddenException,
  isCustomException,
} from '../../../domain/exceptions';
import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../../shared/logger';
import {
  QueryBuilderService,
  validateDeepOptions,
  rewriteFilterDenyingFields,
  rewriteSortDroppingDenied,
} from '@enfyra/kernel';
import {
  RuntimeMetadataSchemaRouterService,
  TableHandlerService,
} from '../../table-management';
import { PolicyService, isPolicyDeny } from '../../../domain/policy';
import { DynamicApiTableValidationService } from '../services/table-validation.service';
import { GuardValidationService } from '../services/guard-validation.service';
import { TDynamicContext } from '../../../shared/types';
import {
  CACHE_EVENTS,
  DATA_EVENTS,
} from '../../../shared/utils/cache-events.constants';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import {
  buildRequestedShapeFromQuery,
  sanitizeFieldPermissionsResult,
} from '../../../shared/utils/sanitize-field-permissions.util';
import {
  decideFieldPermission,
  fieldPermissionRuleAppliesToUser,
  fieldPermissionRuleMatchesSubject,
  formatFieldPermissionErrorMessage,
} from '../../../shared/utils/field-permission.util';
import { UserRevocationService } from '../../../domain/auth';
import {
  normalizeFlowStepScriptConfig,
  normalizeScriptPatch,
  normalizeScriptRecord,
} from '../../../shared/utils/script-code.util';
import { FlowQueueMaintenanceService } from '../../flow';
import { logMemory } from '../../../shared/utils/memory-log.util';
import { normalizeDynamicReadProjection } from '../utils/field-selection.util';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { RuntimeSchemaActivationGateService } from '../../table-management';
import type {
  RuntimeMetadataSchemaMutationResult,
  RuntimeSchemaMetadataTable,
} from '../../table-management/types/runtime-metadata-schema-router.types';
import { TableRouteRouter } from './table-route.router';
import type {
  MutationContext,
  TableRouteStrategy,
} from '../types/table-route.types';

interface DynamicBatchCreateResult {
  accepted: true;
  batch: true;
  count: number;
}

export class DynamicRepository {
  private readonly logger = new Logger('DynamicRepository');
  public context: TDynamicContext;
  private tableName: string;
  private queryBuilderService: QueryBuilderService;
  private tableHandlerService: TableHandlerService;
  private runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
  private policyService: PolicyService;
  private tableValidationService: DynamicApiTableValidationService;
  private guardValidationService: GuardValidationService;
  private eventEmitter: EventEmitter2;
  private userRevocationService?: UserRevocationService;
  private flowQueueMaintenanceService?: FlowQueueMaintenanceService;
  private runtimeRegistryService: RuntimeRegistryService;
  private enforceFieldPermission: boolean;
  private tableMetadata: any;
  private readonly runtimeSchemaActivationGateService?: RuntimeSchemaActivationGateService;
  private readonly routeRouter: TableRouteRouter;

  constructor({
    context,
    tableName,
    queryBuilderService,
    tableHandlerService,
    runtimeMetadataSchemaRouterService,
    policyService,
    tableValidationService,
    guardValidationService,
    eventEmitter,
    userRevocationService,
    flowQueueMaintenanceService,
    runtimeRegistryService,
    enforceFieldPermission,
    runtimeSchemaActivationGateService,
  }: {
    context: TDynamicContext;
    tableName: string;
    queryBuilderService: QueryBuilderService;
    tableHandlerService: TableHandlerService;
    runtimeMetadataSchemaRouterService: RuntimeMetadataSchemaRouterService;
    policyService: PolicyService;
    tableValidationService: DynamicApiTableValidationService;
    guardValidationService: GuardValidationService;
    eventEmitter: EventEmitter2;
    fieldPermissionCacheBuilder?: unknown;
    userRevocationService?: UserRevocationService;
    flowQueueMaintenanceService?: FlowQueueMaintenanceService;
    runtimeRegistryService: RuntimeRegistryService;
    enforceFieldPermission?: boolean;
    runtimeSchemaActivationGateService?: RuntimeSchemaActivationGateService;
  }) {
    this.context = context;
    this.tableName = tableName;
    this.queryBuilderService = queryBuilderService;
    this.tableHandlerService = tableHandlerService;
    this.runtimeMetadataSchemaRouterService =
      runtimeMetadataSchemaRouterService;
    this.policyService = policyService;
    this.tableValidationService = tableValidationService;
    this.guardValidationService = guardValidationService;
    this.eventEmitter = eventEmitter;
    this.userRevocationService = userRevocationService;
    this.flowQueueMaintenanceService = flowQueueMaintenanceService;
    this.runtimeRegistryService = runtimeRegistryService;
    this.enforceFieldPermission = enforceFieldPermission === true;
    this.runtimeSchemaActivationGateService =
      runtimeSchemaActivationGateService;
    this.routeRouter = this.buildRouteRouter();
  }

  private buildRouteRouter(): TableRouteRouter {
    return new TableRouteRouter({
      isSchemaRoutedTable: (tableName) =>
        this.runtimeMetadataSchemaRouterService.handles(tableName),
      isTableDefinition: (tableName) => tableName === 'enfyra_table',
      normalizeRouteMethods: (body, existing, field) =>
        this.filterMethodsSubsetOfAvailable(body, existing, field),
      normalizeExtension: async (body, method) => {
        const { processExtensionDefinition } =
          await import('../../extension-definition/utils/processor.util');
        const { processedBody } = await processExtensionDefinition(
          body,
          method,
        );
        Object.assign(body, processedBody);
      },
      assertColumnRuleUnique: async (body, editingId) =>
        this.assertColumnRuleUnique(body, editingId),
      assertGuardCreate: async (body) =>
        this.guardValidationService.assertGuardCreate(body),
      assertGuardUpdate: async (id, body) =>
        this.guardValidationService.assertGuardUpdate(id, body),
      assertGuardRuleCreate: async (body) =>
        this.guardValidationService.assertGuardRuleBody(body),
      assertGuardRuleUpdate: async (id, body) =>
        this.guardValidationService.assertGuardRuleUpdate(id, body),
      assertFlowTriggerBody: (body) => this.assertFlowTriggerBody(body),
      postStorageDefault: async (currentId) =>
        this.clearOtherDefaultStorageConfigs(currentId),
      postFlowJobs: async (id, name) =>
        this.flowQueueMaintenanceService?.removeFlowJobs({ id, name }),
      postUserRevocation: async (id) => this.userRevocationService?.publish(id),
    });
  }

  async init() {
    this.tableMetadata = await this.lookupActiveTableByName(this.tableName);
  }

  private async ensureInit() {
    if (!this.tableMetadata) {
      this.tableMetadata = await this.lookupActiveTableByName(this.tableName);
    }
  }

  private async getActiveMetadata(): Promise<any> {
    return this.runtimeRegistryService.requireMetadata();
  }

  private async lookupActiveTableByName(
    tableName: string,
  ): Promise<any | null> {
    return this.runtimeRegistryService.lookupTableByName(tableName);
  }

  private getIdField(): string {
    return this.queryBuilderService.getPkField();
  }

  private toScriptBadRequest(error: any): BadRequestException {
    const message = error?.message || String(error) || 'Invalid script source';
    return new BadRequestException(`Invalid script source: ${message}`, {
      code: error?.code || error?.name || 'SCRIPT_VALIDATION_ERROR',
    });
  }

  private normalizeScriptRecordOrThrow(body: Record<string, any>) {
    try {
      return normalizeScriptRecord(this.tableName, body);
    } catch (error) {
      throw this.toScriptBadRequest(error);
    }
  }

  private normalizeScriptPatchOrThrow(
    body: Record<string, any>,
    existing: Record<string, any>,
  ) {
    try {
      return normalizeScriptPatch(this.tableName, body, existing);
    } catch (error) {
      throw this.toScriptBadRequest(error);
    }
  }

  private normalizeFlowStepScriptConfigOrThrow(body: Record<string, any>) {
    try {
      return normalizeFlowStepScriptConfig(body);
    } catch (error) {
      throw this.toScriptBadRequest(error);
    }
  }

  private isPlainObject(value: any): value is Record<string, any> {
    return (
      value !== null &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) === Object.prototype
    );
  }

  private isFilterOperatorObject(value: any): boolean {
    return (
      this.isPlainObject(value) &&
      Object.keys(value).length > 0 &&
      Object.keys(value).every((key) => key.startsWith('_'))
    );
  }

  private normalizeExistsFilter(input: any): any {
    if (!this.isPlainObject(input)) return input;
    const keys = Object.keys(input);
    if (
      keys.length === 1 &&
      Object.prototype.hasOwnProperty.call(input, 'filter') &&
      !this.isFilterOperatorObject(input.filter)
    ) {
      return input.filter;
    }
    return input;
  }

  private hasNonEmptyFilter(value: any): boolean {
    if (value === undefined || value === null) return false;
    if (Array.isArray(value)) {
      return value.some((item) => this.hasNonEmptyFilter(item));
    }
    if (!this.isPlainObject(value)) return true;
    const keys = Object.keys(value);
    if (keys.length === 0) return false;
    return keys.some((key) => this.hasNonEmptyFilter(value[key]));
  }

  private containsUndefined(value: any): boolean {
    if (value === undefined) return true;
    if (Array.isArray(value)) {
      return value.some((item) => this.containsUndefined(item));
    }
    if (!this.isPlainObject(value)) return false;
    return Object.values(value).some((item) => this.containsUndefined(item));
  }

  private assertValidExistsFilter(filter: any) {
    if (filter === undefined || filter === null) {
      throw new BadRequestException('exists requires a non-empty filter');
    }
    if (this.containsUndefined(filter)) {
      throw new BadRequestException(
        'exists filter cannot contain undefined values',
      );
    }
    if (!this.hasNonEmptyFilter(filter)) {
      throw new BadRequestException('exists requires a non-empty filter');
    }
  }

  private getItemId(item: any): any {
    if (item == null) return null;
    if (typeof item === 'string' || typeof item === 'number') return item;
    return item?._id ?? item?.id ?? null;
  }

  private async clearOtherDefaultStorageConfigs(currentId?: string | number) {
    if (this.tableName !== 'enfyra_storage_config') return;

    const idField = this.getIdField();
    const result = await this.queryBuilderService.find({
      table: this.tableName,
      filter: { isDefault: { _eq: true } },
      fields: [idField],
      limit: -1,
    });

    for (const row of result.data || []) {
      const rowId = row?.[idField] ?? row?.id ?? row?._id;
      if (rowId === null || rowId === undefined) continue;
      if (
        currentId !== undefined &&
        currentId !== null &&
        String(rowId) === String(currentId)
      ) {
        continue;
      }
      await this.queryBuilderService.update(this.tableName, rowId, {
        isDefault: false,
      });
    }
  }

  private stripNonUpdatableColumns(data: any, tableMetadata: any): any {
    if (!data || typeof data !== 'object' || !tableMetadata?.columns) {
      return data;
    }

    const stripped = { ...data };
    for (const column of tableMetadata.columns) {
      if (column.isUpdatable === false && column.name in stripped) {
        delete stripped[column.name];
      }
    }
    return stripped;
  }

  private stripUnpublishedEmptyFields(data: any, tableMetadata: any): any {
    if (!data || typeof data !== 'object' || !tableMetadata?.columns) {
      return data;
    }

    const stripped = { ...data };
    for (const column of tableMetadata.columns) {
      if (column.isPublished === false && column.name in stripped) {
        const value = stripped[column.name];
        const isStringLike = [
          'varchar',
          'text',
          'uuid',
          'ObjectId',
          'enum',
          'simple-json',
          'code',
          'array-select',
          'richtext',
          'date',
          'datetime',
          'timestamp',
        ].includes(column.type);
        const isEmpty =
          value === null ||
          value === undefined ||
          (isStringLike && value === '');
        if (isEmpty) {
          delete stripped[column.name];
        }
      }
    }
    return stripped;
  }

  private async assertQueryAllowed() {
    if (!this.enforceFieldPermission) return;
    if (this.context?.$user?.isRootAdmin) return;

    const meta = await this.lookupActiveTableByName(this.tableName);
    if (!meta) return;

    const policies =
      this.runtimeRegistryService.getFieldPermissionPoliciesFor?.(
        this.context.$user,
        this.tableName,
        'read',
      ) ?? [];

    const deniedQueryFields: Array<{
      type: 'column' | 'relation';
      name: string;
    }> = [];

    const queryColumns = new Set<string>();
    const queryRelations = new Set<string>();
    const checkColumn = (name: string) => {
      const col = meta.columns?.find((c: any) => c.name === name);
      if (!col) return;
      queryColumns.add(name);
    };

    const checkRelation = (name: string) => {
      const rel = meta.relations?.find((r: any) => r.propertyName === name);
      if (!rel) return;
      queryRelations.add(name);
    };

    const checkField = (name: string) => {
      checkColumn(name);
      checkRelation(name);
    };

    const filter = this.context.$query?.filter;
    const sort = this.context.$query?.sort;
    const walkFilter = (node: any) => {
      if (!node || typeof node !== 'object') return;
      if (Array.isArray(node)) {
        node.forEach(walkFilter);
        return;
      }
      if (Array.isArray(node._and)) node._and.forEach(walkFilter);
      if (Array.isArray(node._or)) node._or.forEach(walkFilter);
      if (node._not && typeof node._not === 'object') walkFilter(node._not);
      for (const k of Object.keys(node)) {
        if (k === '_and' || k === '_or' || k === '_not') continue;
        if (k.includes('.')) {
          const [first] = k.split('.');
          if (first) checkRelation(first);
        } else {
          checkField(k);
        }
      }
    };

    walkFilter(filter);

    const sortArr = Array.isArray(sort)
      ? sort
      : typeof sort === 'string'
        ? sort
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean)
        : [];
    for (const s of sortArr) {
      const clean = s.startsWith('-') ? s.slice(1) : s;
      if (!clean) continue;
      if (clean.includes('.')) {
        const [first] = clean.split('.');
        if (first) checkRelation(first);
      } else {
        checkField(clean);
      }
    }

    const checkQueryField = async (subjectType: 'column' | 'relation', name: string) => {
      const subject = subjectType === 'column'
        ? meta.columns?.find((c: any) => c.name === name)
        : meta.relations?.find((r: any) => r.propertyName === name);
      if (!subject) return;
      const rules = policies.flatMap((policy: any) => policy.rules || [])
        .filter((rule: any) => rule.isEnabled === true)
        .filter((rule: any) => fieldPermissionRuleMatchesSubject(rule, {
          tableName: this.tableName,
          action: 'read',
          subjectType,
          subjectName: name,
        }))
        .filter((rule: any) => fieldPermissionRuleAppliesToUser(rule, this.context.$user));
      if (rules.length === 0) {
        if (subject.isPublished === false) deniedQueryFields.push({ type: subjectType, name });
        return;
      }
      if (rules.some((rule: any) => rule.condition != null)) {
        deniedQueryFields.push({ type: subjectType, name });
        return;
      }
      const decision = await decideFieldPermission(
        this.runtimeRegistryService,
        {
          user: this.context.$user,
          tableName: this.tableName,
          action: 'read',
          subjectType,
          subjectName: name,
          record: null,
        },
        { defaultAllowed: subject.isPublished !== false },
      );
      if (!decision.allowed) deniedQueryFields.push({ type: subjectType, name });
    };

    await Promise.all([
      ...[...queryColumns].map((name) => checkQueryField('column', name)),
      ...[...queryRelations].map((name) => checkQueryField('relation', name)),
    ]);

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

  private async assertEncryptedQueryFieldsAllowed(
    tableName: string,
    filter: any,
    sort: string | string[] | undefined,
    deep: Record<string, any>,
  ): Promise<void> {
    const metadata = await this.getActiveMetadata();
    this.assertEncryptedFilterFields(tableName, filter, metadata);
    this.assertEncryptedSortFields(tableName, sort, metadata);

    const tableMeta = metadata?.tables?.get(tableName);
    for (const [relationName, entry] of Object.entries(deep || {})) {
      const relation = tableMeta?.relations?.find(
        (rel: any) => rel.propertyName === relationName,
      );
      const targetTable = relation?.targetTableName || relation?.targetTable;
      if (!targetTable || !entry || typeof entry !== 'object') continue;
      await this.assertEncryptedQueryFieldsAllowed(
        targetTable,
        (entry as any).filter,
        (entry as any).sort,
        (entry as any).deep || {},
      );
    }
  }

  private assertEncryptedFilterFields(
    tableName: string,
    filter: any,
    metadata: any,
  ): void {
    if (!filter || typeof filter !== 'object') return;
    if (Array.isArray(filter)) {
      for (const item of filter) {
        this.assertEncryptedFilterFields(tableName, item, metadata);
      }
      return;
    }

    const tableMeta = metadata?.tables?.get(tableName);
    for (const [key, value] of Object.entries(filter)) {
      if (key === '_and' || key === '_or' || key === '_not') {
        this.assertEncryptedFilterFields(tableName, value, metadata);
        continue;
      }
      if (key.startsWith('_')) continue;

      const relation = tableMeta?.relations?.find(
        (rel: any) => rel.propertyName === key,
      );
      if (relation) {
        const targetTable = relation.targetTableName || relation.targetTable;
        if (targetTable) {
          this.assertEncryptedFilterFields(targetTable, value, metadata);
        }
        continue;
      }

      const column = tableMeta?.columns?.find((col: any) => col.name === key);
      if (column?.isEncrypted === true) {
        throw new BadRequestException(
          `Encrypted field '${key}' on '${tableName}' cannot be used for filter.`,
        );
      }
    }
  }

  private assertEncryptedSortFields(
    tableName: string,
    sort: string | string[] | undefined,
    metadata: any,
  ): void {
    if (!sort) return;

    const tokens = Array.isArray(sort)
      ? sort
      : sort
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean);

    for (const token of tokens) {
      const path = token.startsWith('-') ? token.slice(1) : token;
      if (!path || path.startsWith('_count(')) continue;

      const parts = path.split('.');
      let currentTable = tableName;
      let field = parts[0];

      for (let index = 0; index < parts.length; index++) {
        field = parts[index];
        const isLast = index === parts.length - 1;
        if (isLast) break;

        const tableMeta = metadata?.tables?.get(currentTable);
        const relation = tableMeta?.relations?.find(
          (rel: any) => rel.propertyName === field,
        );
        if (!relation) break;
        currentTable =
          relation.targetTableName || relation.targetTable || currentTable;
      }

      const tableMeta = metadata?.tables?.get(currentTable);
      const column = tableMeta?.columns?.find((col: any) => col.name === field);
      if (column?.isEncrypted === true) {
        throw new BadRequestException(
          `Encrypted field '${field}' on '${currentTable}' cannot be used for sort.`,
        );
      }
    }
  }

  private async hasConditionalRulesForField(
    tableName: string,
    action: 'read' | 'create' | 'update',
    subjectType: 'column' | 'relation',
    subjectName: string,
  ): Promise<boolean> {
    const policies =
      this.runtimeRegistryService.getFieldPermissionPoliciesFor?.(
        this.context.$user,
        tableName,
        action,
      ) ?? [];
    for (const p of policies) {
      for (const r of p.rules) {
        if (r.condition == null) continue;
        if (r.tableName !== tableName || r.action !== action) continue;
        if (subjectType === 'column' && r.columnName === subjectName)
          return true;
        if (
          subjectType === 'relation' &&
          r.relationPropertyName === subjectName
        )
          return true;
      }
    }
    return false;
  }

  private async stripDeniedFields(
    tableName: string,
    fields: string | string[] | undefined,
    deep: Record<string, any> | undefined,
  ): Promise<{
    fields: string | string[] | undefined;
    deep: Record<string, any> | undefined;
    needsPostSql: boolean;
  }> {
    if (!this.enforceFieldPermission) {
      return { fields, deep, needsPostSql: false };
    }

    const meta = await this.lookupActiveTableByName(tableName);
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
          ? fields
              .split(',')
              .map((s) => s.trim())
              .filter(Boolean)
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
        this.runtimeRegistryService,
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
          const hasConditional = await this.hasConditionalRulesForField(
            tableName,
            'read',
            'column',
            colName,
          );
          if (!hasConditional) deniedColumns.add(colName);
          else hasConditionalPending = true;
        }
      }
    }

    const deniedRelations = new Set<string>();
    for (const relName of relationsToCheck) {
      const rel = (meta.relations || []).find(
        (r: any) => r.propertyName === relName,
      );
      const defaultAllowed = rel?.isPublished !== false;
      const decision = await decideFieldPermission(
        this.runtimeRegistryService,
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
          const hasConditional = await this.hasConditionalRulesForField(
            tableName,
            'read',
            'relation',
            relName,
          );
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

    const cleanDeep: Record<string, any> | undefined = deep
      ? { ...deep }
      : undefined;
    if (cleanDeep) {
      for (const rel of deniedRelations) {
        delete cleanDeep[rel];
      }
      for (const relName of Object.keys(cleanDeep)) {
        const relEntry = cleanDeep[relName];
        if (!relEntry || typeof relEntry !== 'object') continue;
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

        const _isAllowed = (
          _tblName: string,
          _fieldName: string,
          _fieldType: 'column' | 'relation',
        ) => {
          return true;
        };

        let cleanedFilter = relEntry.filter;
        let cleanedSort = relEntry.sort;

        if (this.enforceFieldPermission && !this.context?.$user?.isRootAdmin) {
          const targetMeta = await this.lookupActiveTableByName(targetTable);
          if (targetMeta) {
            const fullMetadata = await this.getActiveMetadata();

            if (relEntry.filter) {
              cleanedFilter = rewriteFilterDenyingFields(
                relEntry.filter,
                targetTable,
                fullMetadata,
                (tblName, fieldName, fieldType) => {
                  const tMeta = fullMetadata?.tables?.get(tblName);
                  if (!tMeta) return true;
                  if (fieldType === 'column') {
                    const col = tMeta.columns?.find(
                      (c: any) => c.name === fieldName,
                    );
                    return col?.isPublished !== false;
                  } else {
                    const rel = tMeta.relations?.find(
                      (r: any) => r.propertyName === fieldName,
                    );
                    return rel?.isPublished !== false;
                  }
                },
              );
            }

            if (relEntry.sort) {
              const fullMetadata2 = await this.getActiveMetadata();
              cleanedSort = rewriteSortDroppingDenied(
                relEntry.sort,
                targetTable,
                fullMetadata2,
                (tblName, fieldName, fieldType) => {
                  const tMeta = fullMetadata2?.tables?.get(tblName);
                  if (!tMeta) return true;
                  if (fieldType === 'column') {
                    const col = tMeta.columns?.find(
                      (c: any) => c.name === fieldName,
                    );
                    return col?.isPublished !== false;
                  } else {
                    const rel = tMeta.relations?.find(
                      (r: any) => r.propertyName === fieldName,
                    );
                    return rel?.isPublished !== false;
                  }
                },
              );
            }
          }
        }

        cleanDeep[relName] = {
          ...relEntry,
          ...(nested.fields !== relEntry.fields
            ? { fields: nested.fields }
            : {}),
          ...(nested.deep !== relEntry.deep ? { deep: nested.deep } : {}),
          ...(cleanedFilter !== relEntry.filter
            ? { filter: cleanedFilter }
            : {}),
          ...(cleanedSort !== relEntry.sort ? { sort: cleanedSort } : {}),
        };
      }
    }

    return {
      fields: cleanFields,
      deep: cleanDeep,
      needsPostSql: hasConditionalPending,
    };
  }

  async find(
    opt: {
      filter?: any;
      fields?: string | string[];
      limit?: number;
      sort?: string;
      meta?: string | string[];
      aggregate?: any;
      deep?: Record<string, any>;
    } = {},
  ) {
    await this.ensureInit();
    await this.assertQueryAllowed();

    const rawFields = opt?.fields || this.context.$query?.fields;
    const rawDeep: Record<string, any> =
      opt && 'deep' in opt ? opt.deep || {} : this.context.$query?.deep || {};
    const metadata = await this.getActiveMetadata();
    const projection = normalizeDynamicReadProjection({
      tableName: this.tableName,
      fields: rawFields,
      deep: rawDeep,
      metadata,
    });
    const projectedFields = projection.fields;
    const projectedDeep = projection.deep || {};

    if (projectedDeep && Object.keys(projectedDeep).length > 0) {
      validateDeepOptions(
        this.tableName,
        projectedDeep,
        metadata,
        0,
        this.runtimeRegistryService.getMaxQueryDepth(),
      );
    }

    const {
      fields: cleanFields,
      deep: cleanDeep,
      needsPostSql,
    } = await this.stripDeniedFields(
      this.tableName,
      projectedFields,
      projectedDeep,
    );

    const debugMode =
      this.context.$query?.debugMode === 'true' ||
      this.context.$query?.debugMode === true;
    const filterValue = opt?.filter ?? this.context.$query?.filter ?? {};
    const sortValue =
      opt?.sort || this.context.$query?.sort || this.getIdField();
    await this.assertEncryptedQueryFieldsAllowed(
      this.tableName,
      filterValue,
      sortValue,
      cleanDeep || {},
    );
    const result = await this.queryBuilderService.find({
      table: this.tableName,
      fields: cleanFields || '',
      filter: filterValue,
      page: this.context.$query?.page || 1,
      limit:
        opt && 'limit' in opt ? opt.limit : (this.context.$query?.limit ?? 10),
      meta: opt?.meta || this.context.$query?.meta,
      aggregate: opt?.aggregate || this.context.$query?.aggregate,
      sort: sortValue,
      deep: cleanDeep || {},
      debugMode: debugMode,
      debugTrace: this.context.$debug || undefined,
      maxQueryDepth: this.runtimeRegistryService.getMaxQueryDepth(),
    });

    if (!needsPostSql) {
      return result;
    }

    const requested = buildRequestedShapeFromQuery({
      fields: projectedFields,
      deep: projectedDeep,
    });

    const sanitizedData = await sanitizeFieldPermissionsResult({
      value: result?.data ?? [],
      tableName: this.tableName,
      user: this.context.$user,
      action: 'read',
      fieldPermissionPolicyReader: this.runtimeRegistryService,
      metadata,
      requested,
    });

    return {
      ...result,
      data: sanitizedData,
    };
  }

  async exists(filter?: any): Promise<boolean> {
    const normalizedFilter = this.normalizeExistsFilter(filter);
    this.assertValidExistsFilter(normalizedFilter);
    const result = await this.find({
      filter: normalizedFilter,
      fields: [this.getIdField()],
      limit: 1,
      sort: this.getIdField(),
    });
    return Array.isArray(result?.data) && result.data.length > 0;
  }

  async create(opt: {
    data: any;
    fields?: string | string[];
    batch?: boolean;
  }): Promise<any | DynamicBatchCreateResult> {
    await this.ensureInit();
    const startedAt = Date.now();
    const writeMeta = {
      table: this.tableName,
      operation: 'create',
      batch: opt.batch === true,
    };
    logMemory(this.logger, 'dynamic create start', writeMeta);
    try {
      const { data, fields, batch } = opt;
      if (batch) {
        const result = await this.createBatch(data);
        logMemory(this.logger, 'dynamic create batch done', {
          ...writeMeta,
          durationMs: Date.now() - startedAt,
          count: result.count,
        });
        this.emitTableMutation('create', undefined, undefined);
        return result;
      }

      const strategy = this.routeRouter.getStrategy(this.tableName);
      const body = await this.prepareCreateBody(data, strategy);
      logMemory(this.logger, 'dynamic create body prepared', {
        ...writeMeta,
        bodyKeys: Object.keys(body).length,
      });

      const ctx: MutationContext = {
        tableName: this.tableName,
        id: body.id ?? body._id,
        body,
        existing: null,
      };

      if (strategy.kind === 'schema') {
        const mutation = await this.runtimeMetadataSchemaRouterService.create({
          tableName: this.tableName as RuntimeSchemaMetadataTable,
          data: body,
          context: this.context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, {
          ids: mutation.recordId == null ? undefined : [mutation.recordId],
        });
        return this.find({
          filter: {
            [this.getIdField()]: { _eq: mutation.recordId },
          },
          fields,
        });
      }

      if (strategy.kind === 'table') {
        body.isSystem = false;
        const mutation =
          await this.runtimeMetadataSchemaRouterService.createTable({
            body: body as any,
            context: this.context,
          });
        if (mutation.preview) return { data: [mutation.preview] };
        const idValue = mutation.recordId;
        await this.activateRuntimeSchemaMutation(mutation, {
          ids: idValue == null ? undefined : [idValue],
        });
        const result = await this.find({
          filter: { [this.getIdField()]: { _eq: idValue } },
          fields,
        });
        return result;
      }

      const inserted = await this.executeCreateBody(body);
      logMemory(this.logger, 'dynamic create persisted', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
      const createdId = inserted.id || inserted._id || body.id;
      ctx.id = createdId;
      await strategy.afterCreateWrite?.(ctx);
      try {
        const result = await this.find({
          filter: { [this.getIdField()]: { _eq: createdId } },
          fields,
        });
        logMemory(this.logger, 'dynamic create result loaded', {
          ...writeMeta,
          durationMs: Date.now() - startedAt,
        });
        await this.reload({ ids: [createdId] });
        logMemory(this.logger, 'dynamic create done', {
          ...writeMeta,
          durationMs: Date.now() - startedAt,
        });
        this.emitTableMutation('create', [createdId], body);
        return result;
      } catch (error: any) {
        const errorMessage = error?.message || error?.toString() || '';
        if (
          errorMessage.includes('operator does not exist') ||
          errorMessage.includes('character varying')
        ) {
          await this.reload({ ids: [createdId] });
          logMemory(this.logger, 'dynamic create done', {
            ...writeMeta,
            durationMs: Date.now() - startedAt,
            fallbackResult: true,
          });
          this.emitTableMutation('create', [createdId], body);
          return {
            data: [inserted],
            count: 1,
          };
        }
        throw error;
      }
    } catch (error: any) {
      if (isCustomException(error)) {
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

  private async createBatch(data: any): Promise<DynamicBatchCreateResult> {
    if (this.runtimeMetadataSchemaRouterService.handles(this.tableName)) {
      throw new BadRequestException(
        `Batch create is not supported for ${this.tableName}. Use single-record operations.`,
      );
    }
    if (this.tableName === 'enfyra_table') {
      throw new BadRequestException('Batch create is not supported for tables');
    }

    const rows = Array.isArray(data) ? data : [data];
    if (rows.length === 0) {
      throw new BadRequestException('data must contain at least one record');
    }

    const preparedRows: Record<string, any>[] = [];
    const strategy = this.routeRouter.getStrategy(this.tableName);
    for (const row of rows) {
      preparedRows.push(await this.prepareCreateBody(row, strategy));
    }

    for (const body of preparedRows) {
      await (this.queryBuilderService as any).insert(this.tableName, body, {
        batch: true,
      });
    }

    return {
      accepted: true,
      batch: true,
      count: preparedRows.length,
    };
  }

  private async prepareCreateBody(
    raw: any,
    strategy?: TableRouteStrategy,
  ): Promise<Record<string, any>> {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
      throw new BadRequestException('data is required and must be an object');
    }

    const body = { ...raw };
    await this.assertDirectFieldPermission('create', body);

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
    await strategy?.normalizeCreate?.(body);
    Object.assign(body, this.normalizeScriptRecordOrThrow(body));
    if (this.tableName === 'enfyra_flow_step') {
      Object.assign(body, this.normalizeFlowStepScriptConfigOrThrow(body));
    }
    if (body.id !== undefined) {
      delete body.id;
    }
    if (body._id !== undefined) {
      delete body._id;
    }
    return body;
  }

  private async executeCreateBody(body: Record<string, any>): Promise<any> {
    return await this.wrapWithFieldPermissionCheck(() =>
      this.queryBuilderService.runWithPolicy(
        (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
        () => this.queryBuilderService.insert(this.tableName, body),
      ),
    );
  }

  async update(opt: {
    id: string | number;
    data: any;
    fields?: string | string[];
  }) {
    await this.ensureInit();
    const startedAt = Date.now();
    const writeMeta = {
      table: this.tableName,
      operation: 'update',
      id: opt.id,
    };
    logMemory(this.logger, 'dynamic update start', writeMeta);
    try {
      const { id, fields } = opt;
      const originalBody = opt.data;
      let body = this.stripNonUpdatableColumns(originalBody, this.tableMetadata);
      body = this.stripUnpublishedEmptyFields(body, this.tableMetadata);
      logMemory(this.logger, 'dynamic update body stripped', {
        ...writeMeta,
        bodyKeys:
          body && typeof body === 'object' ? Object.keys(body).length : 0,
      });
      const existsResult = await this.find({
        filter: { [this.getIdField()]: { _eq: id } },
      });
      const canonicalExistsResult = await this.queryBuilderService.find({
        table: this.tableName,
        fields: '*',
        filter: { [this.getIdField()]: { _eq: id } },
        limit: 1,
      });
      const exists = canonicalExistsResult?.data?.[0] ?? existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic update existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });

      await this.assertDirectFieldPermission('update', body, exists);

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
      const strategy = this.routeRouter.getStrategy(this.tableName);
      await strategy.normalizeUpdate?.(body, exists, id);
      Object.assign(body, this.normalizeScriptPatchOrThrow(body, exists));
      if (this.tableName === 'enfyra_flow_step') {
        const normalizedFlowStep = this.normalizeFlowStepScriptConfigOrThrow({
          ...exists,
          ...body,
        });
        if ('sourceCode' in normalizedFlowStep) {
          body.sourceCode = normalizedFlowStep.sourceCode;
        }
        if ('scriptLanguage' in normalizedFlowStep) {
          body.scriptLanguage = normalizedFlowStep.scriptLanguage;
        }
        if ('compiledCode' in normalizedFlowStep) {
          body.compiledCode = normalizedFlowStep.compiledCode;
        }
        if ('config' in normalizedFlowStep) {
          body.config = normalizedFlowStep.config;
        }
      }

      const ctx: MutationContext = {
        tableName: this.tableName,
        id,
        body,
        existing: exists,
      };

      if (strategy.kind === 'table') {
        const mutation =
          await this.runtimeMetadataSchemaRouterService.updateTable({
            tableId: id,
            body: body as any,
            existing: exists,
            context: this.context,
          });
        if (mutation.preview) return { data: [mutation.preview] };
        const tableId = mutation.recordId ?? id;
        await this.activateRuntimeSchemaMutation(mutation, {
          ids: [tableId],
        });
        const result = await this.find({
          filter: { [this.getIdField()]: { _eq: tableId } },
          fields,
        });
        return result;
      }
      if (strategy.kind === 'schema') {
        const mutation = await this.runtimeMetadataSchemaRouterService.update({
          tableName: this.tableName as RuntimeSchemaMetadataTable,
          recordId: id,
          data: body,
          existing: exists,
          context: this.context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, { ids: [id] });
        return this.find({
          filter: { [this.getIdField()]: { _eq: id } },
          fields,
        });
      }
      await this.wrapWithFieldPermissionCheck(() =>
        this.queryBuilderService.runWithPolicy(
          (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
          () => this.queryBuilderService.update(this.tableName, id, body),
        ),
      );
      await strategy.afterUpdateWrite?.(ctx);
      logMemory(this.logger, 'dynamic update persisted', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
      const result = await this.find({
        filter: { [this.getIdField()]: { _eq: id } },
        fields,
      });
      logMemory(this.logger, 'dynamic update result loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
      await this.reload({ ids: [id] });
      logMemory(this.logger, 'dynamic update done', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
      await strategy.afterUpdateReload?.(ctx);
      this.emitTableMutation('update', [id], body);
      return result;
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  async delete(opt: { id: string | number }) {
    await this.ensureInit();
    const startedAt = Date.now();
    const writeMeta = {
      table: this.tableName,
      operation: 'delete',
      id: opt.id,
    };
    logMemory(this.logger, 'dynamic delete start', writeMeta);
    try {
      const { id } = opt;
      const idField = this.getIdField();
      const existsResult = await this.find({
        filter: { [idField]: { _eq: id } },
      });
      const exists = existsResult?.data?.[0];
      if (!exists) throw new BadRequestException(`id ${id} is not exists!`);
      logMemory(this.logger, 'dynamic delete existing loaded', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
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
      const strategy = this.routeRouter.getStrategy(this.tableName);
      const ctx: MutationContext = {
        tableName: this.tableName,
        id,
        body: {},
        existing: exists,
      };
      if (strategy.kind === 'table') {
        const mutation =
          await this.runtimeMetadataSchemaRouterService.deleteTable({
            tableId: id,
            existing: exists,
            context: this.context,
          });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, { ids: [id] });
        return { message: 'Success', statusCode: 200 };
      }
      if (strategy.kind === 'schema') {
        const mutation = await this.runtimeMetadataSchemaRouterService.delete({
          tableName: this.tableName as RuntimeSchemaMetadataTable,
          recordId: id,
          existing: exists,
          context: this.context,
        });
        if (mutation.preview) return { data: [mutation.preview] };
        await this.activateRuntimeSchemaMutation(mutation, { ids: [id] });
        return { message: 'Success', statusCode: 200 };
      }
      await this.queryBuilderService.runWithPolicy(
        (tbl, op, d) => this.cascadePolicyCheck(tbl, op, d),
        () => this.queryBuilderService.delete(this.tableName, id),
      );
      await strategy.afterDeleteWrite?.(ctx);
      await this.reload({ ids: [id] });
      logMemory(this.logger, 'dynamic delete done', {
        ...writeMeta,
        durationMs: Date.now() - startedAt,
      });
      await strategy.afterDeleteReload?.(ctx);
      this.emitTableMutation('delete', [id]);
      return { message: 'Delete successfully!', statusCode: 200 };
    } catch (error: any) {
      if (isCustomException(error)) {
        throw error;
      }
      throw new BadRequestException(error.message);
    }
  }

  private toMethodIds(arr: any[]): string[] {
    if (!Array.isArray(arr)) return [];
    return arr
      .map((item) => this.getItemId(item))
      .filter((id) => id != null)
      .map((id) => String(id));
  }

  private filterMethodsSubsetOfAvailable(
    body: any,
    existing: any,
    field: 'publicMethods' | 'skipRoleGuardMethods',
  ): void {
    const availableIds = new Set<string>(
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
      body[field] = [];
      return;
    }
    const current = Array.isArray(body[field]) ? body[field] : [];
    const filtered = current.filter((item: any) => {
      const id = this.getItemId(item);
      return id != null && availableIds.has(String(id));
    });
    body[field] = filtered;
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

  private async assertDirectFieldPermission(
    action: 'create' | 'update',
    body: any,
    existing?: any,
  ): Promise<void> {
    if (!this.enforceFieldPermission || this.context?.$user?.isRootAdmin) {
      return;
    }

    const meta = await this.lookupActiveTableByName(this.tableName);
    if (!meta) return;

    const record = action === 'update' ? existing : body;
    const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
    for (const key of Object.keys(body || {})) {
      const col = meta.columns?.find((c: any) => c.name === key);
      if (col) {
        if (action === 'update' && col.isUpdatable === false) continue;
        const decision = await decideFieldPermission(
          this.runtimeRegistryService,
          {
            user: this.context.$user,
            tableName: this.tableName,
            action,
            subjectType: 'column',
            subjectName: key,
            record,
          },
          { defaultAllowed: col.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'column', name: key });
      }

      const rel = meta.relations?.find((r: any) => r.propertyName === key);
      if (rel) {
        const decision = await decideFieldPermission(
          this.runtimeRegistryService,
          {
            user: this.context.$user,
            tableName: this.tableName,
            action,
            subjectType: 'relation',
            subjectName: key,
            record,
          },
          { defaultAllowed: rel.isPublished !== false },
        );
        if (!decision.allowed) denied.push({ type: 'relation', name: key });
      }
    }

    if (denied.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({
          action,
          tableName: this.tableName,
          fields: denied,
        }),
      );
    }
  }

  private async cascadeFieldPermissionCheck(
    tableName: string,
    action: 'create' | 'update',
    data: any,
  ): Promise<void> {
    if (!this.enforceFieldPermission) return;
    const meta = await this.lookupActiveTableByName(tableName);
    if (!meta) return;
    const denied: Array<{ type: 'column' | 'relation'; name: string }> = [];
    for (const key of Object.keys(data || {})) {
      const col = meta.columns?.find((c: any) => c.name === key);
      if (col) {
        if (action === 'update' && col.isUpdatable === false) continue;
        const decision = await decideFieldPermission(
          this.runtimeRegistryService,
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
          this.runtimeRegistryService,
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
        formatFieldPermissionErrorMessage({
          action,
          tableName,
          fields: denied,
        }),
      );
    }
  }

  private wrapWithFieldPermissionCheck<T>(
    callback: () => Promise<T>,
  ): Promise<T> {
    if (!this.enforceFieldPermission || this.context?.$user?.isRootAdmin) {
      return callback();
    }
    return this.queryBuilderService.runWithFieldPermissionCheck(
      (tbl, action, d) => this.cascadeFieldPermissionCheck(tbl, action, d),
      callback,
    );
  }

  private async assertColumnRuleUnique(
    body: any,
    editingId: string | number | null,
  ): Promise<void> {
    const ruleType = body?.ruleType;
    if (!ruleType || ruleType === 'custom') return;

    const columnRef = body?.column;
    const columnId =
      columnRef && typeof columnRef === 'object'
        ? (columnRef.id ?? columnRef._id)
        : columnRef;
    if (columnId == null) return;

    const existing = await this.queryBuilderService.find({
      table: 'enfyra_column_rule',
      filter: {
        ruleType: { _eq: ruleType },
        column: { id: { _eq: columnId } },
      },
      fields: [this.getIdField()],
      limit: 10,
    });
    const rows: any[] = existing?.data ?? [];
    const conflict = rows.find(
      (r) => String(r[this.getIdField()]) !== String(editingId ?? ''),
    );
    if (conflict) {
      throw new ConflictException(
        `Rule of type '${ruleType}' already exists for this column`,
        {
          ruleType,
          columnId: String(columnId),
          existingId: conflict[this.getIdField()],
        },
      );
    }
  }

  private async activateRuntimeSchemaMutation(
    mutation: RuntimeMetadataSchemaMutationResult,
    opts: { ids?: (string | number)[] },
  ): Promise<void> {
    const mutationId = mutation.mutationId;
    if (!mutationId) {
      await this.reload({
        ids: opts.ids,
        affectedTables: mutation.affectedTables,
        critical: true,
        tableRenames: mutation.tableRenames,
      });
      return;
    }

    this.runtimeSchemaActivationGateService?.begin(mutationId);
    let lastError: unknown;
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      try {
        await this.reload({
          ids: opts.ids,
          affectedTables: mutation.affectedTables,
          critical: true,
          tableRenames: mutation.tableRenames,
        });
        await this.runtimeMetadataSchemaRouterService.markActivated(mutationId);
        this.runtimeSchemaActivationGateService?.complete(mutationId);
        return;
      } catch (error) {
        lastError = error;
        if (attempt < 3) {
          await new Promise((resolve) => setTimeout(resolve, attempt * 100));
        }
      }
    }

    this.runtimeSchemaActivationGateService?.fail(mutationId, lastError);
    const message =
      lastError instanceof Error ? lastError.message : String(lastError);
    throw new Error(
      `Schema mutation committed but cache activation failed; instance fenced: ${message}`,
    );
  }

  private async reload(opts?: {
    ids?: (string | number)[];
    affectedTables?: string[];
    tableRenames?: TCacheInvalidationPayload['tableRenames'];
    critical?: boolean;
  }) {
    const payload: TCacheInvalidationPayload = {
      table: this.tableName,
      action: 'reload',
      timestamp: Date.now(),
      scope: opts?.ids?.length ? 'partial' : 'full',
      ids: opts?.ids,
      affectedTables: opts?.affectedTables,
      critical: opts?.critical,
      tableRenames: opts?.tableRenames,
    };
    if (typeof this.eventEmitter.emitAsync === 'function') {
      await this.eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, payload);
      return;
    }
    this.eventEmitter.emit(CACHE_EVENTS.INVALIDATE, payload);
  }

  private emitTableMutation(
    action: 'create' | 'update' | 'delete',
    ids?: (string | number)[],
    data?: any,
  ) {
    this.eventEmitter.emit(DATA_EVENTS.TABLE_MUTATION, {
      table: this.tableName,
      action,
      ids,
      data,
      userId: this.context?.$user?.id ?? null,
    });
  }

  private assertFlowTriggerBody(body: any) {
    const type = body.type;
    if (!type || !['schedule', 'event', 'webhook'].includes(type)) {
      throw new BadRequestException(
        'Flow trigger type must be one of: schedule, event, webhook',
      );
    }
    if (type === 'schedule') {
      const config =
        typeof body.config === 'string' ? JSON.parse(body.config) : body.config;
      if (!config?.cron)
        throw new BadRequestException('Schedule trigger requires config.cron');
    }
    if (type === 'event') {
      if (!body.table && !body.tableId)
        throw new BadRequestException('Event trigger requires table reference');
      if (
        !body.tableEvent ||
        !['create', 'update', 'delete'].includes(body.tableEvent)
      ) {
        throw new BadRequestException(
          'Event trigger requires tableEvent (create|update|delete)',
        );
      }
    }
    if (type === 'webhook') {
      if (!body.route && !body.routeId)
        throw new BadRequestException(
          'Webhook trigger requires route reference',
        );
    }
  }
}
