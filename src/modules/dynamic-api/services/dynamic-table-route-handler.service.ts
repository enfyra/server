import {
  BadRequestException,
  ConflictException,
} from '../../../domain/exceptions';
import { autoSlug } from '../../../shared/utils/auto-slug.helper';
import type { TableRouteHandlers } from '../types/table-route.types';
import type { DynamicTableRouteHandlerDependencies } from '../types/dynamic-table-route-handler.types';

export class DynamicTableRouteHandlerService implements TableRouteHandlers {
  constructor(
    private readonly dependencies: DynamicTableRouteHandlerDependencies,
  ) {}

  isSchemaRoutedTable(tableName: string): boolean {
    return this.dependencies.runtimeMetadataSchemaRouterService.handles(tableName);
  }

  isTableDefinition(tableName: string): boolean {
    return tableName === 'enfyra_table';
  }

  normalizeRouteMethods(
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
    body[field] = current.filter((item: any) => {
      const id = this.getItemId(item);
      return id != null && availableIds.has(String(id));
    });
  }

  async normalizeExtension(
    body: any,
    method: 'POST' | 'PATCH',
  ): Promise<void> {
    const { processExtensionDefinition } =
      await import('../../extension-definition/utils/processor.util');
    const { processedBody } = await processExtensionDefinition(body, method);
    Object.assign(body, processedBody);
  }

  async assertColumnRuleUnique(
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

    const idField = this.dependencies.queryBuilderService.getPkField();
    const existing = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_column_rule',
      filter: {
        ruleType: { _eq: ruleType },
        column: { id: { _eq: columnId } },
      },
      fields: [idField],
      limit: 10,
    });
    const rows: any[] = existing?.data ?? [];
    const conflict = rows.find(
      (row) => String(row[idField]) !== String(editingId ?? ''),
    );
    if (conflict) {
      throw new ConflictException(
        `Rule of type '${ruleType}' already exists for this column`,
        {
          ruleType,
          columnId: String(columnId),
          existingId: conflict[idField],
        },
      );
    }
  }

  async assertGuardCreate(body: any): Promise<void> {
    await this.dependencies.guardValidationService.assertGuardCreate(body);
  }

  async assertGuardUpdate(id: string | number, body: any): Promise<void> {
    await this.dependencies.guardValidationService.assertGuardUpdate(id, body);
  }

  async assertGuardRuleCreate(body: any): Promise<void> {
    await this.dependencies.guardValidationService.assertGuardRuleBody(body);
  }

  async assertGuardRuleUpdate(id: string | number, body: any): Promise<void> {
    await this.dependencies.guardValidationService.assertGuardRuleUpdate(id, body);
  }

  assertFlowTriggerBody(body: any): void {
    const type = body.type;
    if (!type || !['schedule', 'event', 'webhook'].includes(type)) {
      throw new BadRequestException(
        'Flow trigger type must be one of: schedule, event, webhook',
      );
    }
    if (type === 'schedule') {
      const config =
        typeof body.config === 'string' ? JSON.parse(body.config) : body.config;
      if (!config?.cron) {
        throw new BadRequestException('Schedule trigger requires config.cron');
      }
    }
    if (type === 'event') {
      if (!body.table && !body.tableId) {
        throw new BadRequestException('Event trigger requires table reference');
      }
      if (
        !body.tableEvent ||
        !['create', 'update', 'delete'].includes(body.tableEvent)
      ) {
        throw new BadRequestException(
          'Event trigger requires tableEvent (create|update|delete)',
        );
      }
    }
    if (type === 'webhook' && !body.route && !body.routeId) {
      throw new BadRequestException('Webhook trigger requires route reference');
    }
  }

  async normalizeUserPassword(body: Record<string, any>): Promise<void> {
    if (!body.password || typeof body.password !== 'string') return;
    if (/^\$2[aby]\$\d{2}\$/.test(body.password)) return;
    body.password = await this.dependencies.bcryptService.hash(body.password);
  }

  normalizeFolderSlug(body: Record<string, any>): void {
    if (body.name) body.slug = autoSlug(String(body.name));
  }

  async postStorageDefault(currentId: string | number): Promise<void> {
    const result = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_storage_config',
      filter: { isDefault: { _eq: true } },
      fields: [this.dependencies.queryBuilderService.getPkField()],
      limit: 0,
    });

    const idField = this.dependencies.queryBuilderService.getPkField();
    for (const row of result.data || []) {
      const rowId = row?.[idField] ?? row?.id ?? row?._id;
      if (rowId === null || rowId === undefined) continue;
      if (String(rowId) === String(currentId)) continue;
      await this.dependencies.queryBuilderService.update(
        'enfyra_storage_config',
        rowId,
        { isDefault: false },
      );
    }
  }

  async postFlowJobs(id: string | number, name: string): Promise<unknown> {
    return this.dependencies.flowQueueMaintenanceService?.removeFlowJobs({
      id,
      name,
    });
  }

  async postUserRevocation(id: string | number): Promise<unknown> {
    return this.dependencies.userRevocationService?.publish(id);
  }

  async createRouteMethodConfigsForRoute(
    routeId: string | number,
    isSystem: boolean,
    routeState: Record<string, any> = {},
  ): Promise<void> {
    const idField = this.dependencies.queryBuilderService.getPkField();
    const methods = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_method',
      fields: [idField],
      limit: 0,
    });
    await this.createMissingRouteMethodConfigs(
      methods.data.map((method: any) => ({
        routeId,
        methodId: method[idField],
        isSystem,
      })),
    );
    await this.syncRouteMethodConfigFlags(routeId, routeState);
  }

  async createRouteMethodConfigsForMethod(
    methodId: string | number,
  ): Promise<void> {
    const idField = this.dependencies.queryBuilderService.getPkField();
    const routes = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_route',
      fields: [idField, 'isSystem'],
      limit: 0,
    });
    await this.createMissingRouteMethodConfigs(
      routes.data.map((route: any) => ({
        routeId: route[idField],
        methodId,
        isSystem: route.isSystem === true,
      })),
    );
  }

  async syncRouteMethodConfigFlags(
    routeId: string | number,
    routeState: Record<string, any>,
  ): Promise<void> {
    const idField = this.dependencies.queryBuilderService.getPkField();
    const routeResult = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_route',
      filter: { [idField]: { _eq: routeId } },
      fields: [
        idField,
        'availableMethods.id',
        'publicMethods.id',
        'skipRoleGuardMethods.id',
      ],
      limit: 1,
    });
    const currentRoute = routeResult.data?.[0] ?? routeState;
    const available = new Set(
      this.toMethodIds(
        Array.isArray(currentRoute.availableMethods)
          ? currentRoute.availableMethods
          : [],
      ),
    );
    const publicMethods = new Set(
      this.toMethodIds(
        Array.isArray(currentRoute.publicMethods)
          ? currentRoute.publicMethods
          : [],
      ),
    );
    const skipRoleGuard = new Set(
      this.toMethodIds(
        Array.isArray(currentRoute.skipRoleGuardMethods)
          ? currentRoute.skipRoleGuardMethods
          : [],
      ),
    );
    const configs = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_route_method_config',
      filter: { route: { [idField]: { _eq: routeId } } },
      fields: [idField, 'method.id'],
      limit: 0,
    });

    for (const config of configs.data) {
      const configId = config[idField];
      const methodId = this.getItemId(config.method);
      if (configId == null || methodId == null) continue;
      const key = String(methodId);
      await this.dependencies.queryBuilderService.update(
        'enfyra_route_method_config',
        configId,
        {
          available: available.has(key),
          isPublic: publicMethods.has(key),
          skipRoleGuard: skipRoleGuard.has(key),
        },
      );
    }
  }

  async removeIncompleteRouteMethodMatrix(
    tableName: 'enfyra_route' | 'enfyra_method',
    id: string | number,
  ): Promise<void> {
    await this.dependencies.queryBuilderService.delete(tableName, id);
  }

  assertRouteMethodConfigCreateAllowed(): never {
    throw new BadRequestException(
      'Route method configurations are created automatically for every route and method.',
    );
  }

  assertRouteMethodConfigUpdate(body: Record<string, any>): void {
    if (
      Object.prototype.hasOwnProperty.call(body, 'route') ||
      Object.prototype.hasOwnProperty.call(body, 'method')
    ) {
      throw new BadRequestException(
        'Route and method cannot be changed on a route method configuration.',
      );
    }
  }

  assertRouteMethodConfigDeleteAllowed(): never {
    throw new BadRequestException(
      'Route method configurations are permanent matrix cells. Disable the configuration instead of deleting it.',
    );
  }

  async normalizeRouteHandlerConfig(
    body: Record<string, any>,
    existing: Record<string, any> | null = null,
  ): Promise<void> {
    const route = body.route ?? existing?.route;
    const method = body.method ?? existing?.method;
    const routeId = this.getItemId(route);
    const methodId = this.getItemId(method);
    if (routeId == null || methodId == null) {
      throw new BadRequestException(
        'Route handler requires route and method references.',
      );
    }

    const idField = this.dependencies.queryBuilderService.getPkField();
    const configs = await this.dependencies.queryBuilderService.find({
      table: 'enfyra_route_method_config',
      filter: {
        _and: [
          { route: { [idField]: { _eq: routeId } } },
          { method: { [idField]: { _eq: methodId } } },
        ],
      },
      fields: [idField],
      limit: 1,
    });
    const config = configs.data?.[0];
    if (!config?.[idField]) {
      throw new BadRequestException(
        'Route method configuration is missing for this handler.',
      );
    }
    body.routeMethodConfig = { [idField]: config[idField] };
  }

  async syncRouteHandlerTimeout(
    body: Record<string, any>,
    existing: Record<string, any> | null = null,
  ): Promise<void> {
    if (!Object.prototype.hasOwnProperty.call(body, 'timeout')) return;
    const configId = this.getItemId(
      body.routeMethodConfig ?? existing?.routeMethodConfig,
    );
    if (configId == null) return;
    const timeout = this.normalizeRouteHandlerTimeout(body.timeout);
    await this.dependencies.queryBuilderService.update(
      'enfyra_route_method_config',
      configId,
      { timeout },
    );
  }

  private normalizeRouteHandlerTimeout(value: unknown): number {
    const timeout = Number(value);
    if (!Number.isFinite(timeout) || timeout === 0) return 60_000;
    return Math.max(1, Math.trunc(timeout));
  }

  private async createMissingRouteMethodConfigs(
    pairs: Array<{
      routeId: string | number;
      methodId: string | number;
      isSystem: boolean;
    }>,
  ): Promise<void> {
    const idField = this.dependencies.queryBuilderService.getPkField();
    for (const pair of pairs) {
      if (pair.routeId == null || pair.methodId == null) continue;
      try {
        await this.dependencies.queryBuilderService.insertWithOptions({
          table: 'enfyra_route_method_config',
          data: {
            route: { [idField]: pair.routeId },
            method: { [idField]: pair.methodId },
            available: false,
            isPublic: false,
            skipRoleGuard: false,
            timeout: 30_000,
            requestBodyType: 'none',
            isSystem: pair.isSystem,
          },
        });
      } catch (error) {
        if (!this.isDuplicateRouteMethodConfig(error)) throw error;
      }
    }
  }

  private isDuplicateRouteMethodConfig(error: any): boolean {
    return (
      error?.code === '23505' ||
      error?.code === 'ER_DUP_ENTRY' ||
      error?.errno === 1062 ||
      error?.code === 11000
    );
  }

  private getItemId(item: any): any {
    if (item == null) return null;
    if (typeof item === 'string' || typeof item === 'number') return item;
    return item?._id ?? item?.id ?? null;
  }

  private toMethodIds(items: any[]): string[] {
    if (!Array.isArray(items)) return [];
    return items
      .map((item) => this.getItemId(item))
      .filter((id) => id != null)
      .map((id) => String(id));
  }
}
