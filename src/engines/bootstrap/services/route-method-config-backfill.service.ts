import { QueryBuilderService } from '@enfyra/kernel';
import { ObjectId } from 'mongodb';
import { replaceMongoJunctionRows } from '../../../domain/bootstrap/utils/mongo-junction-writer.util';
import { replaceSqlJunctionRows } from '../../../domain/bootstrap/utils/sql-junction-writer.util';
import { Logger } from '../../../shared/logger';
import type { MetadataCacheService } from '../../cache';
import type {
  LegacyRouteHandlerPair,
  LegacyRouteMethodPair,
  RouteMethodConfigBackfillDraft,
} from '../types/route-method-config-backfill.types';
import { buildRouteMethodConfigBackfillDrafts } from '../utils/route-method-config-backfill.util';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';

interface RouteMethodConfigBackfillState {
  routes: any[];
  methods: any[];
  configs: any[];
  handlers: any[];
  permissions: any[];
  preHooks: any[];
  postHooks: any[];
  guards: any[];
}

export class RouteMethodConfigBackfillService {
  private readonly logger = new Logger(RouteMethodConfigBackfillService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;
  private metadata: any;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
  }

  async run(): Promise<void> {
    this.metadata = await this.metadataCacheService.getMetadata();
    const state = await this.loadState();
    const drafts = this.buildDrafts(state);
    const existingConfigCount = state.configs.length;
    const validPairs = new Set(
      drafts.map((draft) => this.pairKey(draft.routeId, draft.methodId)),
    );
    const existingPairs = state.configs.map((config) =>
      this.pairKey(this.relationId(config.route), this.relationId(config.method)),
    );
    if (
      new Set(existingPairs).size !== existingPairs.length ||
      existingPairs.some((pair) => !validPairs.has(pair))
    ) {
      throw new Error(
        'Route method configuration matrix contains duplicate or orphan cells',
      );
    }
    const migratingLegacyBindings = existingConfigCount === 0;
    this.verbose(
      `Route method config matrix: routes=${state.routes.length}, methods=${state.methods.length}, expected=${drafts.length}, existing=${existingConfigCount}`,
    );
    const configuredRoutes = new Set(
      state.configs.map((config) => String(this.relationId(config.route))),
    );
    const configByPair = new Map(
      state.configs.map((config) => [
        this.pairKey(
          this.relationId(config.route),
          this.relationId(config.method),
        ),
        config,
      ]),
    );

    for (const draft of drafts) {
      const key = this.pairKey(draft.routeId, draft.methodId);
      const newCell =
        existingConfigCount > 0 && configuredRoutes.has(String(draft.routeId));
      const config = await this.upsertConfig(
        newCell
          ? {
              ...draft,
              available: false,
              isPublic: false,
              skipRoleGuard: false,
              timeout: 30_000,
            }
          : draft,
        configByPair.get(key),
      );
      configByPair.set(key, config);
      if (draft.handlerId != null && migratingLegacyBindings) {
        await this.attachHandler(draft.handlerId, config);
      }
    }

    if (migratingLegacyBindings) {
      await this.attachRouteScopedBindings(state, configByPair);
    }
    this.assertCompleteMatrix(drafts, configByPair);
    this.verbose(
      `Route method config matrix materialized: total=${drafts.length}, created=${Math.max(0, drafts.length - existingConfigCount)}, handlers=${state.handlers.length}, permissions=${state.permissions.length}, preHooks=${state.preHooks.length}, postHooks=${state.postHooks.length}, guards=${state.guards.length}`,
    );
    this.logger.debug(
      `Backfilled ${drafts.length} route method configuration(s)`,
    );
  }

  private async loadState(): Promise<RouteMethodConfigBackfillState> {
    const [
      routes,
      methods,
      configs,
      handlers,
      permissions,
      preHooks,
      postHooks,
      guards,
    ] = await Promise.all([
      this.loadRawRecords('enfyra_route', [
        'availableMethods',
        'publicMethods',
        'skipRoleGuardMethods',
      ]),
      this.loadRawRecords('enfyra_method'),
      this.loadRawRecords('enfyra_route_method_config', ['route', 'method']),
      this.loadRawRecords('enfyra_route_handler', [
        'route',
        'method',
        'routeMethodConfig',
      ]),
      this.loadRawRecords('enfyra_route_permission', [
        'route',
        'methods',
        'routeMethodConfigs',
      ]),
      this.loadRawRecords('enfyra_pre_hook', [
        'route',
        'methods',
        'routeMethodConfigs',
      ]),
      this.loadRawRecords('enfyra_post_hook', [
        'route',
        'methods',
        'routeMethodConfigs',
      ]),
      this.loadRawRecords('enfyra_guard', [
        'parent',
        'route',
        'methods',
        'routeMethodConfigs',
      ]),
    ]);

    return {
      routes,
      methods,
      configs,
      handlers,
      permissions,
      preHooks,
      postHooks,
      guards,
    };
  }

  private buildDrafts(
    state: RouteMethodConfigBackfillState,
  ): RouteMethodConfigBackfillDraft[] {
    const routeIds: unknown[] = [];
    const methodIds: unknown[] = [];
    const available: LegacyRouteMethodPair[] = [];
    const publicPairs: LegacyRouteMethodPair[] = [];
    const skipRoleGuard: LegacyRouteMethodPair[] = [];
    const handlers: LegacyRouteHandlerPair[] = [];
    const systemRouteIds: unknown[] = [];

    for (const route of state.routes) {
      const routeId = this.recordId(route);
      if (routeId == null) continue;
      routeIds.push(routeId);
      if (this.isTrue(route.isSystem)) systemRouteIds.push(routeId);
      this.collectRoutePairs(available, routeId, route.availableMethods);
      this.collectRoutePairs(publicPairs, routeId, route.publicMethods);
      this.collectRoutePairs(skipRoleGuard, routeId, route.skipRoleGuardMethods);
    }

    for (const method of state.methods) {
      const methodId = this.recordId(method);
      if (methodId != null) methodIds.push(methodId);
    }

    for (const handler of state.handlers) {
      const routeId = this.relationId(handler.route);
      const methodId = this.relationId(handler.method);
      const handlerId = this.recordId(handler);
      if (routeId == null || methodId == null || handlerId == null) continue;
      handlers.push({ routeId, methodId, handlerId, timeout: handler.timeout });
    }

    return buildRouteMethodConfigBackfillDrafts({
      routeIds,
      methodIds,
      flags: { available, public: publicPairs, skipRoleGuard },
      handlers,
      systemRouteIds,
    });
  }

  private async upsertConfig(
    draft: RouteMethodConfigBackfillDraft,
    existing: any,
  ): Promise<any> {
    if (existing) return existing;

    const routeRelation = this.requireRelation(
      'enfyra_route_method_config',
      'route',
    );
    const methodRelation = this.requireRelation(
      'enfyra_route_method_config',
      'method',
    );
    const data = {
      [routeRelation.foreignKeyColumn]: draft.routeId,
      [methodRelation.foreignKeyColumn]: draft.methodId,
      available: draft.available,
      isPublic: draft.isPublic,
      skipRoleGuard: draft.skipRoleGuard,
      timeout: draft.timeout,
      requestBodyType: 'none',
      isSystem: draft.isSystem,
    };

    if (this.queryBuilderService.isMongoDb()) {
      const result = await this.queryBuilderService
        .getMongoDb()
        .collection('enfyra_route_method_config')
        .insertOne(data);
      return {
        _id: result.insertedId,
        route: draft.routeId,
        method: draft.methodId,
      };
    }

    const knex = this.queryBuilderService.getKnex();
    await knex('enfyra_route_method_config').insert(data);
    const inserted = await knex('enfyra_route_method_config')
      .where({
        [routeRelation.foreignKeyColumn]: draft.routeId,
        [methodRelation.foreignKeyColumn]: draft.methodId,
      })
      .first();
    if (!inserted) {
      throw new Error(
        `Failed to read inserted route method configuration for route ${String(draft.routeId)} and method ${String(draft.methodId)}`,
      );
    }
    return {
      ...inserted,
      route: draft.routeId,
      method: draft.methodId,
    };
  }

  private async attachHandler(handlerId: unknown, config: any): Promise<void> {
    const configId = this.recordId(config);
    if (configId == null) return;
    await this.updateRawRecord(
      'enfyra_route_handler',
      handlerId,
      this.requireRelation('enfyra_route_handler', 'routeMethodConfig')
        .foreignKeyColumn,
      configId,
    );
  }

  private async attachRouteScopedBindings(
    state: RouteMethodConfigBackfillState,
    configByPair: Map<string, any>,
  ): Promise<void> {
    for (const permission of state.permissions) {
      await this.attachExplicitConfigs(
        'enfyra_route_permission',
        permission,
        configByPair,
      );
    }
    for (const hook of [
      ...state.preHooks.filter((record) => !this.isTrue(record.isGlobal)),
      ...state.postHooks.filter((record) => !this.isTrue(record.isGlobal)),
    ]) {
      await this.attachExplicitConfigs(
        state.preHooks.includes(hook) ? 'enfyra_pre_hook' : 'enfyra_post_hook',
        hook,
        configByPair,
      );
    }
    for (const guard of state.guards.filter(
      (record) =>
        record.type !== 'graphql' &&
        record.parent == null &&
        !this.isTrue(record.isGlobal) &&
        this.relationId(record.route) != null,
    )) {
      const methods = this.relationList(guard.methods);
      if (methods.length === 0) {
        await this.updateRawScalar(
          'enfyra_guard',
          this.recordId(guard),
          'appliesToAllRouteMethods',
          true,
        );
        await this.replaceRelationRows(
          'enfyra_guard',
          'routeMethodConfigs',
          this.recordId(guard),
          [],
        );
        continue;
      }
      await this.attachExplicitConfigs('enfyra_guard', guard, configByPair);
    }
  }

  private async attachExplicitConfigs(
    table: string,
    record: any,
    configByPair: Map<string, any>,
  ): Promise<void> {
    const recordId = this.recordId(record);
    const routeId = this.relationId(record.route);
    if (recordId == null || routeId == null) return;
    const configIds = this.relationList(record.methods)
      .map((method) =>
        configByPair.get(this.pairKey(routeId, this.relationId(method))),
      )
      .map((config) => this.recordId(config))
      .filter((id) => id != null);
    await this.replaceRelationRows(
      table,
      'routeMethodConfigs',
      recordId,
      configIds,
    );
  }

  private async loadRawRecords(
    tableName: string,
    relationNames: string[] = [],
  ): Promise<any[]> {
    const rows = this.queryBuilderService.isMongoDb()
      ? await this.queryBuilderService
          .getMongoDb()
          .collection(tableName)
          .find({})
          .toArray()
      : await this.queryBuilderService.getKnex()(tableName).select('*');
    if (relationNames.length === 0) return rows;

    for (const relationName of relationNames) {
      const relation = this.findRelation(tableName, relationName);
      if (!relation) continue;
      if (relation.type === 'many-to-many') {
        await this.hydrateManyToMany(rows, relation);
        continue;
      }
      const idField = this.queryBuilderService.getPkField();
      for (const row of rows) {
        const value = row[relation.foreignKeyColumn];
        row[relationName] = value == null ? null : { [idField]: value };
      }
    }
    return rows;
  }

  private async hydrateManyToMany(rows: any[], relation: any): Promise<void> {
    const idField = this.queryBuilderService.getPkField();
    const rowById = new Map(
      rows.map((row) => [String(row[idField]), row]),
    );
    for (const row of rows) row[relation.propertyName] = [];
    if (rows.length === 0) return;

    const junctionRows = this.queryBuilderService.isMongoDb()
      ? await this.queryBuilderService
          .getMongoDb()
          .collection(relation.junctionTableName)
          .find({
            [relation.junctionSourceColumn]: {
              $in: rows.map((row) => row[idField]),
            },
          })
          .toArray()
      : await this.queryBuilderService
          .getKnex()(relation.junctionTableName)
          .whereIn(
            relation.junctionSourceColumn,
            rows.map((row) => row[idField]),
          )
          .select(
            relation.junctionSourceColumn,
            relation.junctionTargetColumn,
          );

    for (const junctionRow of junctionRows) {
      const sourceId = junctionRow[relation.junctionSourceColumn];
      const targetId = junctionRow[relation.junctionTargetColumn];
      const row = rowById.get(String(sourceId));
      if (row && targetId != null) {
        row[relation.propertyName].push({ [idField]: targetId });
      }
    }
  }

  private async replaceRelationRows(
    tableName: string,
    propertyName: string,
    sourceId: unknown,
    targetIds: unknown[],
  ): Promise<void> {
    if (sourceId == null) return;
    const relation = this.requireRelation(tableName, propertyName);
    const input = {
      junctionTable: relation.junctionTableName,
      sourceColumn: relation.junctionSourceColumn,
      targetColumn: relation.junctionTargetColumn,
      sourceId,
      targetIds,
    };
    if (this.queryBuilderService.isMongoDb()) {
      await replaceMongoJunctionRows(this.queryBuilderService, input);
    } else {
      await replaceSqlJunctionRows(this.queryBuilderService, input);
    }
  }

  private async updateRawRecord(
    tableName: string,
    recordId: unknown,
    fieldName: string,
    value: unknown,
  ): Promise<void> {
    await this.updateRawScalar(tableName, recordId, fieldName, value);
  }

  private async updateRawScalar(
    tableName: string,
    recordId: unknown,
    fieldName: string,
    value: unknown,
  ): Promise<void> {
    if (recordId == null) return;
    const idField = this.queryBuilderService.getPkField();
    if (this.queryBuilderService.isMongoDb()) {
      await this.queryBuilderService
        .getMongoDb()
        .collection(tableName)
        .updateOne({ [idField]: recordId }, { $set: { [fieldName]: value } });
      return;
    }
    await this.queryBuilderService
      .getKnex()(tableName)
      .where({ [idField]: recordId })
      .update({ [fieldName]: value });
  }

  private findRelation(tableName: string, propertyName: string): any {
    const table =
      this.metadata?.tables?.get?.(tableName) ??
      this.metadata?.tablesList?.find((candidate: any) => candidate.name === tableName);
    return table?.relations?.find(
      (relation: any) => relation.propertyName === propertyName,
    );
  }

  private requireRelation(tableName: string, propertyName: string): any {
    const relation = this.findRelation(tableName, propertyName);
    if (!relation) {
      throw new Error(
        `Missing relation metadata for ${tableName}.${propertyName}`,
      );
    }
    return relation;
  }

  private assertCompleteMatrix(
    drafts: RouteMethodConfigBackfillDraft[],
    configByPair: Map<string, any>,
  ): void {
    if (configByPair.size !== drafts.length) {
      throw new Error(
        `Route method configuration matrix mismatch: expected ${drafts.length}, received ${configByPair.size}`,
      );
    }
    for (const draft of drafts) {
      const key = this.pairKey(draft.routeId, draft.methodId);
      if (this.recordId(configByPair.get(key)) == null) {
        throw new Error(
          `Missing route method configuration for route ${String(draft.routeId)} and method ${String(draft.methodId)}`,
        );
      }
    }
  }

  private collectRoutePairs(
    target: LegacyRouteMethodPair[],
    routeId: unknown,
    methods: unknown,
  ): void {
    for (const method of this.relationList(methods)) {
      const methodId = this.relationId(method);
      if (methodId != null) target.push({ routeId, methodId });
    }
  }

  private relationList(value: unknown): any[] {
    return Array.isArray(value) ? value : [];
  }

  private relationId(value: any): unknown {
    if (value == null) return null;
    if (value instanceof ObjectId) return value;
    if (typeof value === 'object') return value._id ?? value.id ?? null;
    return value;
  }

  private recordId(value: any): unknown {
    return this.relationId(value);
  }

  private isTrue(value: unknown): boolean {
    return value === true || value === 1;
  }

  private pairKey(routeId: unknown, methodId: unknown): string {
    return `${String(routeId)}\u0000${String(methodId)}`;
  }

  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
