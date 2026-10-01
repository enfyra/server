import type {
  RouteKind,
  TableRouteHandlers,
  TableRouteStrategy,
} from '../types/table-route.types';

export class TableRouteRouter {
  private readonly strategies = new Map<string, TableRouteStrategy>();

  constructor(private readonly handlers: TableRouteHandlers) {
    this.register('enfyra_route', {
      kind: 'generic',
      requiresCriticalReload: true,
      normalizeCreate(body) {
        handlers.normalizeRouteMethods(body, null, 'publicMethods');
        handlers.normalizeRouteMethods(body, null, 'skipRoleGuardMethods');
      },
      normalizeUpdate(body, existing) {
        if (body.publicMethods) {
          handlers.normalizeRouteMethods(body, existing, 'publicMethods');
        }
        if (body.skipRoleGuardMethods) {
          handlers.normalizeRouteMethods(
            body,
            existing,
            'skipRoleGuardMethods',
          );
        }
      },
      async afterCreateWrite(ctx) {
        try {
          await handlers.createRouteMethodConfigsForRoute(
            ctx.id,
            ctx.body.isSystem === true,
            ctx.body,
          );
        } catch (error) {
          await handlers.removeIncompleteRouteMethodMatrix('enfyra_route', ctx.id);
          throw error;
        }
      },
      async afterUpdateWrite(ctx) {
        if (
          ['availableMethods', 'publicMethods', 'skipRoleGuardMethods'].some(
            (field) => Object.prototype.hasOwnProperty.call(ctx.body, field),
          )
        ) {
          await handlers.syncRouteMethodConfigFlags(ctx.id, {
            ...ctx.existing,
            ...ctx.body,
          });
        }
      },
    });
    this.register('enfyra_method', {
      kind: 'generic',
      requiresCriticalReload: true,
      async afterCreateWrite(ctx) {
        try {
          await handlers.createRouteMethodConfigsForMethod(ctx.id);
        } catch (error) {
          await handlers.removeIncompleteRouteMethodMatrix('enfyra_method', ctx.id);
          throw error;
        }
      },
    });
    this.register('enfyra_route_method_config', {
      kind: 'generic',
      requiresCriticalReload: true,
      normalizeCreate() {
        handlers.assertRouteMethodConfigCreateAllowed();
      },
      normalizeUpdate(body) {
        handlers.assertRouteMethodConfigUpdate(body);
      },
      beforeDelete() {
        handlers.assertRouteMethodConfigDeleteAllowed();
      },
    });
    this.register('enfyra_route_handler', {
      kind: 'generic',
      requiresCriticalReload: true,
      async normalizeCreate(body) {
        await handlers.normalizeRouteHandlerConfig(body);
      },
      async normalizeUpdate(body, existing) {
        await handlers.normalizeRouteHandlerConfig(body, existing);
      },
      async afterCreateWrite(ctx) {
        await handlers.syncRouteHandlerTimeout(ctx.body);
      },
      async afterUpdateWrite(ctx) {
        await handlers.syncRouteHandlerTimeout(ctx.body, ctx.existing);
      },
    });
    this.register('enfyra_route_method_config_file_field', {
      kind: 'generic',
      requiresCriticalReload: true,
    });
    this.register('enfyra_extension', {
      kind: 'generic',
      async normalizeCreate(body) {
        await handlers.normalizeExtension(body, 'POST');
      },
      async normalizeUpdate(body) {
        await handlers.normalizeExtension(body, 'PATCH');
      },
    });
    this.register('enfyra_column_rule', {
      kind: 'generic',
      async normalizeCreate(body) {
        await handlers.assertColumnRuleUnique(body, null);
      },
      async normalizeUpdate(body, _existing, id) {
        await handlers.assertColumnRuleUnique(body, id);
      },
    });
    this.register('enfyra_guard', {
      kind: 'generic',
      async normalizeCreate(body) {
        await handlers.assertGuardCreate(body);
      },
      async normalizeUpdate(body, _existing, id) {
        await handlers.assertGuardUpdate(id, body);
      },
    });
    this.register('enfyra_guard_rule', {
      kind: 'generic',
      async normalizeCreate(body) {
        await handlers.assertGuardRuleCreate(body);
      },
      async normalizeUpdate(body, _existing, id) {
        await handlers.assertGuardRuleUpdate(id, body);
      },
    });
    this.register('enfyra_flow_trigger', {
      kind: 'generic',
      normalizeCreate(body) {
        handlers.assertFlowTriggerBody(body);
      },
      normalizeUpdate(body, existing) {
        handlers.assertFlowTriggerBody({ ...existing, ...body });
      },
    });
    this.register('enfyra_storage_config', {
      kind: 'generic',
      async afterCreateWrite(ctx) {
        if (ctx.body.isDefault === true) {
          await handlers.postStorageDefault(ctx.id);
        }
      },
      async afterUpdateWrite(ctx) {
        if (ctx.body.isDefault === true) {
          await handlers.postStorageDefault(ctx.id);
        }
      },
    });
    this.register('enfyra_user', {
      kind: 'generic',
      async normalizeCreate(body) {
        await handlers.normalizeUserPassword(body);
      },
      async normalizeUpdate(body) {
        await handlers.normalizeUserPassword(body);
      },
      async afterUpdateReload(ctx) {
        if (
          ctx.body &&
          (Object.prototype.hasOwnProperty.call(ctx.body, 'password') ||
            Object.prototype.hasOwnProperty.call(ctx.body, 'roles'))
        ) {
          await handlers.postUserRevocation(ctx.id);
        }
      },
      async afterDeleteReload(ctx) {
        await handlers.postUserRevocation(ctx.id);
      },
    });
    this.register('enfyra_folder', {
      kind: 'generic',
      normalizeCreate(body) {
        handlers.normalizeFolderSlug(body);
      },
      normalizeUpdate(body) {
        handlers.normalizeFolderSlug(body);
      },
    });
    this.register('enfyra_flow', {
      kind: 'generic',
      async afterDeleteWrite(ctx) {
        await handlers.postFlowJobs(ctx.id, ctx.existing?.name ?? '');
      },
    });
  }

  private register(tableName: string, strategy: TableRouteStrategy) {
    this.strategies.set(tableName, strategy);
  }

  getStrategy(tableName: string): TableRouteStrategy {
    const strategy = this.strategies.get(tableName);
    if (strategy) return strategy;
    const kind: RouteKind = this.handlers.isTableDefinition(tableName)
      ? 'table'
      : this.handlers.isSchemaRoutedTable(tableName)
        ? 'schema'
        : 'generic';
    return { kind };
  }
}
