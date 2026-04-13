import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';

const GUARD_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.GUARD,
  colorCode: '\x1b[35m',
  cacheName: 'GuardCache',
};

export type GuardRuleType =
  | 'rate_limit_by_ip'
  | 'rate_limit_by_user'
  | 'rate_limit_by_route'
  | 'ip_whitelist'
  | 'ip_blacklist';

export type GuardPosition = 'pre_auth' | 'post_auth';
export type GuardCombinator = 'and' | 'or';

export interface GuardRuleNode {
  id: number;
  type: GuardRuleType;
  config: any;
  priority: number;
  isEnabled: boolean;
  userIds: string[];
}

export interface GuardNode {
  id: number;
  name: string;
  position: GuardPosition | null;
  combinator: GuardCombinator;
  priority: number;
  isEnabled: boolean;
  isGlobal: boolean;
  parentId: number | null;
  routeId: number | null;
  routePath: string | null;
  methodIds: number[];
  methods: string[];
  children: GuardNode[];
  rules: GuardRuleNode[];
}

export interface GuardCache {
  preAuthGlobal: GuardNode[];
  postAuthGlobal: GuardNode[];
  preAuthByRoute: Map<string, GuardNode[]>;
  postAuthByRoute: Map<string, GuardNode[]>;
}

@Injectable()
export class GuardCacheService extends BaseCacheService<GuardCache> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(GUARD_CONFIG, eventEmitter);
    this.cache = {
      preAuthGlobal: [],
      postAuthGlobal: [],
      preAuthByRoute: new Map(),
      postAuthByRoute: new Map(),
    };
  }

  protected async loadFromDb(): Promise<any> {
    const [guardsResult, rulesResult] = await Promise.all([
      this.queryBuilder.find({
        table: 'guard_definition',
        filter: { isEnabled: { _eq: true } },
        fields: ['*', 'parent', 'route.id', 'route.path', 'methods.method'],
        sort: ['priority'],
      }),
      this.queryBuilder.find({
        table: 'guard_rule_definition',
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
    const isMongo = this.queryBuilder.isMongoDb();

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
        ? guard.methods.map((m: any) => m?.method ?? m).filter(Boolean)
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
        if (parent) {
          parent.children.push(node);
        }
      } else {
        roots.push(node);
      }
    }

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
    };

    for (const root of roots) {
      const position = root.position;
      if (!position) continue;

      if (root.isGlobal) {
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
      }
    }

    cache.preAuthGlobal.sort((a, b) => a.priority - b.priority);
    cache.postAuthGlobal.sort((a, b) => a.priority - b.priority);
    for (const list of cache.preAuthByRoute.values())
      list.sort((a, b) => a.priority - b.priority);
    for (const list of cache.postAuthByRoute.values())
      list.sort((a, b) => a.priority - b.priority);

    return cache;
  }

  protected emitLoadedEvent(): void {
    this.eventEmitter?.emit(CACHE_EVENTS.GUARD_LOADED);
  }

  protected getLogCount(): string {
    const total =
      this.cache.preAuthGlobal.length +
      this.cache.postAuthGlobal.length +
      [...this.cache.preAuthByRoute.values()].reduce(
        (s, l) => s + l.length,
        0,
      ) +
      [...this.cache.postAuthByRoute.values()].reduce(
        (s, l) => s + l.length,
        0,
      );
    return `${total} guards`;
  }

  protected getCount(): number {
    return (
      this.cache.preAuthGlobal.length +
      this.cache.postAuthGlobal.length +
      [...this.cache.preAuthByRoute.values()].reduce(
        (s, l) => s + l.length,
        0,
      ) +
      [...this.cache.postAuthByRoute.values()].reduce((s, l) => s + l.length, 0)
    );
  }

  getGuardsForRoute(
    position: GuardPosition,
    routePath: string,
    method: string,
  ): GuardNode[] {
    const globalGuards =
      position === 'pre_auth'
        ? this.cache.preAuthGlobal
        : this.cache.postAuthGlobal;
    const routeMap =
      position === 'pre_auth'
        ? this.cache.preAuthByRoute
        : this.cache.postAuthByRoute;
    const routeGuards = routeMap.get(routePath) || [];

    const all = [...globalGuards, ...routeGuards];
    return all.filter((g) => {
      if (g.methods.length === 0) return true;
      return g.methods.includes(method);
    });
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
}
