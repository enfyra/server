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
      limit: -1,
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
