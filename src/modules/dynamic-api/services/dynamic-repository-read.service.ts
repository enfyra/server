import {
  QueryBuilderService,
  validateDeepOptions,
} from '@enfyra/kernel';
import { BadRequestException } from '../../../domain/exceptions';
import type { TDynamicContext } from '../../../shared/types';
import {
  buildRequestedShapeFromQuery,
  sanitizeFieldPermissionsResult,
} from '../../../shared/utils/sanitize-field-permissions.util';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import { normalizeDynamicReadProjection } from '../utils/field-selection.util';
import type { DynamicReadOptions } from '../types/dynamic-read.types';
import { DynamicReadAuthorizationService } from './dynamic-read-authorization.service';

export class DynamicRepositoryReadService {
  constructor(
    private readonly deps: {
      context: TDynamicContext;
      enforceFieldPermission: boolean;
      queryBuilderService: QueryBuilderService;
      readAuthorizationService: DynamicReadAuthorizationService;
      runtimeRegistryService: RuntimeRegistryService;
      tableName: string;
    },
  ) {}

  async find(options: DynamicReadOptions = {}) {
    const { context, runtimeRegistryService, tableName } = this.deps;
    runtimeRegistryService.lookupTableByName(tableName);
    await this.deps.readAuthorizationService.assertQueryAllowed({
      tableName,
      context,
      enforceFieldPermission: this.deps.enforceFieldPermission,
    });

    const rawFields = options.fields || context.$query?.fields;
    const rawDeep =
      'deep' in options ? options.deep ?? {} : context.$query?.deep ?? {};
    const metadata = runtimeRegistryService.requireMetadata();
    const projection = normalizeDynamicReadProjection({
      tableName,
      fields: rawFields,
      deep: rawDeep,
      metadata,
    });
    const projectedDeep = projection.deep || {};
    if (Object.keys(projectedDeep).length > 0) {
      validateDeepOptions(
        tableName,
        projectedDeep,
        metadata,
        0,
        runtimeRegistryService.getMaxQueryDepth(),
      );
    }

    const prepared = await this.deps.readAuthorizationService.stripDeniedFields({
      tableName,
      fields: projection.fields,
      deep: projectedDeep,
      context,
      enforceFieldPermission: this.deps.enforceFieldPermission,
    });
    const debugMode =
      context.$query?.debugMode === 'true' || context.$query?.debugMode === true;
    const filter = options.filter ?? context.$query?.filter ?? {};
    const sort =
      options.sort || context.$query?.sort || this.deps.queryBuilderService.getPkField();
    await this.deps.readAuthorizationService.assertEncryptedQueryFieldsAllowed(
      tableName,
      filter,
      sort,
      prepared.deep || {},
    );
    const result = await this.deps.queryBuilderService.find({
      table: tableName,
      fields: prepared.fields || '',
      filter,
      page: context.$query?.page || 1,
      limit: 'limit' in options ? options.limit : (context.$query?.limit ?? 10),
      meta: options.meta || context.$query?.meta,
      sort,
      deep: prepared.deep || {},
      debugMode,
      debugTrace: context.$debug || undefined,
      maxQueryDepth: runtimeRegistryService.getMaxQueryDepth(),
    });
    if (!prepared.needsPostSql) return result;

    const data = await sanitizeFieldPermissionsResult({
      value: result?.data ?? [],
      tableName,
      user: context.$user,
      action: 'read',
      fieldPermissionPolicyReader: runtimeRegistryService,
      metadata,
      requested: buildRequestedShapeFromQuery({
        fields: projection.fields,
        deep: projectedDeep,
      }),
    });
    return { ...result, data };
  }

  async aggregate(aggregate: unknown): Promise<any> {
    const { context, runtimeRegistryService, tableName } = this.deps;
    runtimeRegistryService.lookupTableByName(tableName);
    await this.deps.readAuthorizationService.assertQueryAllowed({
      tableName,
      context,
      enforceFieldPermission: this.deps.enforceFieldPermission,
    });
    const metadata = runtimeRegistryService.requireMetadata();
    const filter =
      aggregate && typeof aggregate === 'object' && !Array.isArray(aggregate) && 'filter' in aggregate
        ? (aggregate as Record<string, unknown>).filter
        : undefined;
    const sort =
      aggregate && typeof aggregate === 'object' && !Array.isArray(aggregate) && 'sort' in aggregate
        ? (aggregate as Record<string, unknown>).sort
        : undefined;
    await this.deps.readAuthorizationService.assertAggregateFieldsAllowed(
      tableName,
      aggregate,
      context,
      this.deps.enforceFieldPermission,
    );
    await this.deps.readAuthorizationService.assertEncryptedQueryFieldsAllowed(
      tableName,
      filter,
      Array.isArray(sort)
        ? sort.map((item: any) => `${item.direction === 'desc' ? '-' : ''}${item.field}`).join(',')
        : undefined,
      {},
    );
    const result = await this.deps.queryBuilderService.aggregate({
      table: tableName,
      aggregate: aggregate as any,
      debugMode:
        context.$query?.debugMode === 'true' || context.$query?.debugMode === true,
      debugTrace: context.$debug || undefined,
    });
    if (!this.deps.enforceFieldPermission) return result;
    return result;
  }

  async exists(filter?: unknown): Promise<boolean> {
    const normalizedFilter = this.normalizeExistsFilter(filter);
    this.assertValidExistsFilter(normalizedFilter);
    const primaryKey = this.deps.queryBuilderService.getPkField();
    const result = await this.find({
      filter: normalizedFilter,
      fields: [primaryKey],
      limit: 1,
      sort: primaryKey,
    });
    return Array.isArray(result?.data) && result.data.length > 0;
  }

  private normalizeExistsFilter(input: unknown): unknown {
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

  private assertValidExistsFilter(filter: unknown): void {
    if (filter === undefined || filter === null) {
      throw new BadRequestException('exists requires a non-empty filter');
    }
    if (this.containsUndefined(filter)) {
      throw new BadRequestException('exists filter cannot contain undefined values');
    }
    if (!this.hasNonEmptyFilter(filter)) {
      throw new BadRequestException('exists requires a non-empty filter');
    }
  }

  private isPlainObject(value: unknown): value is Record<string, unknown> {
    return (
      value !== null &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      Object.getPrototypeOf(value) === Object.prototype
    );
  }

  private isFilterOperatorObject(value: unknown): boolean {
    return (
      this.isPlainObject(value) &&
      Object.keys(value).length > 0 &&
      Object.keys(value).every((key) => key.startsWith('_'))
    );
  }

  private hasNonEmptyFilter(value: unknown): boolean {
    if (value === undefined || value === null) return false;
    if (Array.isArray(value)) return value.some((item) => this.hasNonEmptyFilter(item));
    if (!this.isPlainObject(value)) return true;
    const keys = Object.keys(value);
    return keys.length > 0 && keys.some((key) => this.hasNonEmptyFilter(value[key]));
  }

  private containsUndefined(value: unknown): boolean {
    if (value === undefined) return true;
    if (Array.isArray(value)) return value.some((item) => this.containsUndefined(item));
    return this.isPlainObject(value) && Object.values(value).some((item) => this.containsUndefined(item));
  }
}
