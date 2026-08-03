import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import type {
  GuardCache,
  GuardNode,
  GuardPosition,
  GuardRuleNode,
} from '../types/guard.types';

export type {
  GuardCache,
  GuardCombinator,
  GuardNode,
  GuardPosition,
  GuardRuleNode,
  GuardRuleType,
  GuardTargetType,
} from '../types/guard.types';

const GUARD_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.GUARD,
  colorCode: '\x1b[35m',
  cacheName: 'GuardCache',
};

export class GuardCacheBuilder extends BaseCacheService<GuardCache> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(GUARD_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
    this.cache = {
      preAuthGlobal: [],
      postAuthGlobal: [],
      preAuthByRoute: new Map(),
      postAuthByRoute: new Map(),
      gqlPreAuthGlobal: [],
      gqlPostAuthGlobal: [],
      gqlPreAuthByTable: new Map(),
      gqlPostAuthByTable: new Map(),
      gqlPreAuthByOperation: new Map(),
      gqlPostAuthByOperation: new Map(),
      gqlPreAuthExact: new Map(),
      gqlPostAuthExact: new Map(),
    };
  }

  protected async loadFromDb(): Promise<any> {
    const [guardsResult, rulesResult] = await Promise.all([
      this.queryBuilderService.find({
        table: 'enfyra_guard',
        fields: [
          '*',
          'parent',
          'route.id',
          'route.path',
          'table.id',
          'table.name',
          'methods.name',
        ],
        sort: ['priority'],
      }),
      this.queryBuilderService.find({
        table: 'enfyra_guard_rule',
        filter: { isEnabled: { _eq: true } },
        fields: ['*', 'guard', 'users.id'],
        sort: ['priority'],
      }),
    ]);

    return { guards: guardsResult.data, rules: rulesResult.data };
  }

  protected transformData(rawData: {
    guards: any[];
    rules: any[];
  }): GuardCache {
    const { guards, rules } = rawData;
    const isMongo = this.queryBuilderService.isMongoDb();

    const getId = (obj: any): number | null => {
      if (!obj) return null;
      if (isMongo) return obj._id ?? obj.id ?? null;
      return obj.id ?? obj;
    };

    const rulesByGuardId = new Map<number, GuardRuleNode[]>();
    for (const rule of rules) {
      const guardId = getId(rule.guard);
      if (guardId == null) continue;
      const list = rulesByGuardId.get(guardId) || [];
      const userIds: string[] = Array.isArray(rule.users)
        ? rule.users.map((u: any) => String(u?.id ?? u)).filter(Boolean)
        : [];
      list.push({
        id: getId(rule) as number,
        type: rule.type,
        config:
          typeof rule.config === 'string'
            ? JSON.parse(rule.config)
            : rule.config,
        priority: rule.priority ?? 0,
        isEnabled: rule.isEnabled !== false,
        userIds,
      });
      rulesByGuardId.set(guardId, list);
    }

    const nodeMap = new Map<number, GuardNode>();
    for (const guard of guards) {
      const id = getId(guard) as number;
      const methods = Array.isArray(guard.methods)
        ? guard.methods.map((m: any) => m?.name ?? m).filter(Boolean)
        : [];
      const methodIds = Array.isArray(guard.methods)
        ? (guard.methods.map((m: any) => getId(m)).filter(Boolean) as number[])
        : [];

      nodeMap.set(id, {
        id,
        name: guard.name,
        position: guard.position || null,
        combinator: guard.combinator || 'and',
        priority: guard.priority ?? 0,
        isEnabled: guard.isEnabled !== false,
        isGlobal: guard.isGlobal === true,
        type: guard.type === 'graphql' ? 'graphql' : 'route',
        gqlOperation: guard.gqlOperation || null,
        tableName: guard.table?.name || null,
        parentId: getId(guard.parent),
        routeId: guard.route ? getId(guard.route) : null,
        routePath: guard.route?.path || null,
        methodIds,
        methods,
        children: [],
        rules: rulesByGuardId.get(id) || [],
      });
    }

    const roots: GuardNode[] = [];
    for (const node of nodeMap.values()) {
      if (node.parentId != null) {
        const parent = nodeMap.get(node.parentId);
        if (!parent) {
          throw new Error(
            `Guard "${node.name}" (id=${node.id}) parent guard ${node.parentId} does not exist`,
          );
        }
        parent.children.push(node);
      } else {
        roots.push(node);
      }
    }

    this.assertAcyclicGuardForest(nodeMap);

    for (const root of roots) {
      this.validateGuardTree(root, root.position);
    }

    for (const node of nodeMap.values()) {
      node.children.sort((a, b) => a.priority - b.priority);
      node.rules.sort((a, b) => a.priority - b.priority);
    }

    const cache: GuardCache = {
      preAuthGlobal: [],
      postAuthGlobal: [],
      preAuthByRoute: new Map(),
      postAuthByRoute: new Map(),
      gqlPreAuthGlobal: [],
      gqlPostAuthGlobal: [],
      gqlPreAuthByTable: new Map(),
      gqlPostAuthByTable: new Map(),
      gqlPreAuthByOperation: new Map(),
      gqlPostAuthByOperation: new Map(),
      gqlPreAuthExact: new Map(),
      gqlPostAuthExact: new Map(),
    };

    for (const root of roots) {
      if (!root.isEnabled) continue;
      const position = root.position;
      if (!position) {
        throw new Error(
          `Enabled root guard "${root.name}" (id=${root.id}) requires position pre_auth or post_auth`,
        );
      }

      if (root.type === 'graphql') {
        this.classifyGqlGuard(cache, root, position);
        continue;
      }

      if (root.isGlobal) {
        // Global wins: a guard with isGlobal=true applies everywhere, even if
        // a route scope is also present (conflicting data — warn instead of silently picking one).
        if (root.routePath) {
          this.logger.warn(
            `Guard "${root.name}" (id=${root.id}) has both isGlobal=true and route scope — route scope ignored, treated as global`,
          );
        }
        if (position === 'pre_auth') {
          cache.preAuthGlobal.push(root);
        } else {
          cache.postAuthGlobal.push(root);
        }
      } else if (root.routePath) {
        const routeMap =
          position === 'pre_auth'
            ? cache.preAuthByRoute
            : cache.postAuthByRoute;
        const list = routeMap.get(root.routePath) || [];
        list.push(root);
        routeMap.set(root.routePath, list);
      } else {
        this.logger.warn(
          `Guard "${root.name}" (id=${root.id}) has no route scope and isGlobal=false — dropped from runtime cache`,
        );
      }
    }

    cache.preAuthGlobal.sort((a, b) => a.priority - b.priority);
    cache.postAuthGlobal.sort((a, b) => a.priority - b.priority);
    for (const list of cache.preAuthByRoute.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.postAuthByRoute.values())
      list.sort((a, b) => a.priority - b.priority);
    cache.gqlPreAuthGlobal.sort((a, b) => a.priority - b.priority);
    cache.gqlPostAuthGlobal.sort((a, b) => a.priority - b.priority);
    for (const list of cache.gqlPreAuthByTable.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.gqlPostAuthByTable.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.gqlPreAuthByOperation.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.gqlPostAuthByOperation.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.gqlPreAuthExact.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.gqlPostAuthExact.values())
      list.sort((a, b) => a.priority - b.priority);

    return cache;
  }

  /**
   * Classify a GraphQL guard into one of the 4 matrix buckets:
   *   (null, null)      → global
   *   (table, null)     → byTable
   *   (null, op)        → byOperation
   *   (table, op)       → exact `${table}:${op}`
   * GQL never uses isGlobal (matrix is the source of truth).
   */
  private classifyGqlGuard(
    cache: GuardCache,
    root: GuardNode,
    position: GuardPosition,
  ): void {
    const tableName = root.tableName;
    const op = root.gqlOperation;

    if (tableName == null && op == null) {
      const list =
        position === 'pre_auth'
          ? cache.gqlPreAuthGlobal
          : cache.gqlPostAuthGlobal;
      list.push(root);
    } else if (tableName != null && op == null) {
      const map =
        position === 'pre_auth'
          ? cache.gqlPreAuthByTable
          : cache.gqlPostAuthByTable;
      const list = map.get(tableName) || [];
      list.push(root);
      map.set(tableName, list);
    } else if (tableName == null && op != null) {
      const map =
        position === 'pre_auth'
          ? cache.gqlPreAuthByOperation
          : cache.gqlPostAuthByOperation;
      const list = map.get(op) || [];
      list.push(root);
      map.set(op, list);
    } else {
      const map =
        position === 'pre_auth'
          ? cache.gqlPreAuthExact
          : cache.gqlPostAuthExact;
      const key = `${tableName}:${op}`;
      const list = map.get(key) || [];
      list.push(root);
      map.set(key, list);
    }
  }

  protected emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.GUARD_LOADED);
  }

  protected getLogCount(): string {
    const total = this.countAll();
    return `${total} guards`;
  }

  protected getCount(): number {
    return this.countAll();
  }

  private countAll(): number {
    const sum = (arr: GuardNode[]) => arr.length;
    const sumMap = (m: Map<string, GuardNode[]>) =>
      [...m.values()].reduce((s, l) => s + l.length, 0);
    return (
      sum(this.cache.preAuthGlobal) +
      sum(this.cache.postAuthGlobal) +
      sumMap(this.cache.preAuthByRoute) +
      sumMap(this.cache.postAuthByRoute) +
      sum(this.cache.gqlPreAuthGlobal) +
      sum(this.cache.gqlPostAuthGlobal) +
      sumMap(this.cache.gqlPreAuthByTable) +
      sumMap(this.cache.gqlPostAuthByTable) +
      sumMap(this.cache.gqlPreAuthByOperation) +
      sumMap(this.cache.gqlPostAuthByOperation) +
      sumMap(this.cache.gqlPreAuthExact) +
      sumMap(this.cache.gqlPostAuthExact)
    );
  }

  async ensureGuardsLoaded(): Promise<void> {
    await this.ensureLoaded();
  }

  private validateGuardTree(
    node: GuardNode,
    rootPosition: GuardPosition | null,
  ): void {
    const requiresUser = ['rate_limit_by_user'];

    node.rules = node.rules.filter((rule) => {
      if (rootPosition === 'pre_auth' && requiresUser.includes(rule.type)) {
        this.logger.warn(
          `Guard "${node.name}": rule "${rule.type}" (id=${rule.id}) requires post_auth but root guard is pre_auth — skipped`,
        );
        return false;
      }

      if (rootPosition === 'pre_auth' && rule.userIds.length > 0) {
        this.logger.warn(
          `Guard "${node.name}": rule id=${rule.id} has user scope but root guard is pre_auth — user scope ignored`,
        );
        rule.userIds = [];
      }

      return true;
    });

    for (const child of node.children) {
      this.validateGuardTree(child, rootPosition);
    }
  }

  private assertAcyclicGuardForest(nodeMap: Map<number, GuardNode>): void {
    const visiting = new Set<number>();
    const visited = new Set<number>();

    const visit = (node: GuardNode): void => {
      if (visited.has(node.id)) return;
      if (visiting.has(node.id)) {
        throw new Error(
          `Guard parent hierarchy contains a cycle at guard "${node.name}" (id=${node.id})`,
        );
      }
      visiting.add(node.id);
      if (node.parentId != null) {
        const parent = nodeMap.get(node.parentId);
        if (!parent) {
          throw new Error(
            `Guard "${node.name}" (id=${node.id}) parent guard ${node.parentId} does not exist`,
          );
        }
        visit(parent);
      }
      visiting.delete(node.id);
      visited.add(node.id);
    };

    for (const node of nodeMap.values()) visit(node);
  }
}
