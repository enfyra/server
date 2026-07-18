import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { RedisPubSubService } from './redis-pubsub.service';
import {
  InstanceService,
  RuntimeMetricsCollectorService,
} from '../../../shared/services';
import { MetadataCacheService } from './metadata-cache.service';
import { RouteCacheService } from './route-cache.service';
import { GuardCacheBuilder } from './guard-cache-builder.service';
import { FlowCacheBuilder } from './flow-cache-builder.service';
import { WebsocketCacheBuilder } from './websocket-cache-builder.service';
import { PackageCacheService } from './package-cache.service';
import { SettingCacheService } from './setting-cache.service';
import { StorageConfigCacheBuilder } from './storage-config-cache-builder.service';
import { OAuthConfigCacheBuilder } from './oauth-config-cache-builder.service';
import { FolderTreeCacheService } from './folder-tree-cache.service';
import { FieldPermissionCacheBuilder } from './field-permission-cache-builder.service';
import { ColumnRuleCacheBuilder } from './column-rule-cache-builder.service';
import { GqlDefinitionCacheService } from './gql-definition-cache.service';
import { RepoRegistryService } from './repo-registry.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import {
  RuntimeRegistryService,
  type RuntimeCacheViewSource,
} from './runtime-registry.service';
import { RuntimeReloadAuditService } from './runtime-reload-audit.service';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { logMemory } from '../../../shared/utils/memory-log.util';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
import { DynamicWebSocketGateway } from '../../../modules/websocket';
import { GraphqlService } from '../../../modules/graphql';
import { BootstrapScriptService } from '../../../domain/bootstrap';
import { LifecycleAware } from '../../../shared/interfaces/lifecycle-aware.interface';
import type {
  RuntimeCacheIdentifier,
  RuntimeRegistrySnapshot,
} from '../types/runtime-registry.types';

const COLOR = '\x1b[33m';
const RESET = '\x1b[0m';
const SYNC_CHANNEL = 'enfyra:cache-orchestrator-sync';
const FULL_RELOAD_BUILDER_CONCURRENCY = 3;

const FLOW_PRIORITY = [
  'metadata',
  'route',
  'fieldPermission',
  'setting',
  'guard',
  'flow',
  'websocket',
  'storage',
  'oauth',
  'folder',
  'package',
  'menu',
  'extension',
  'bootstrap',
  'graphql',
  'settingGraphql',
  'repoRegistry',
];

type ReloadStep = (
  payload: TCacheInvalidationPayload,
  options?: { sharedReplay?: boolean },
) => Promise<void>;
type CacheReloadStepRunner = (
  name: string,
  fn: () => Promise<void>,
) => Promise<void>;
type CacheReloadStepMetric = {
  name: string;
  durationMs: number;
  status: 'success' | 'failed';
  error?: string;
};

export const RELOAD_CHAINS: Record<string, string[]> = {
  [SYSTEM_TABLES.table]: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
  ],
  [SYSTEM_TABLES.column]: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
  ],
  [SYSTEM_TABLES.relation]: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
  ],

  [SYSTEM_TABLES.route]: ['route', 'graphql', 'guard'],
  [SYSTEM_TABLES.preHook]: ['route'],
  [SYSTEM_TABLES.postHook]: ['route'],
  [SYSTEM_TABLES.routeHandler]: ['route'],
  [SYSTEM_TABLES.routePermission]: ['route'],
  [SYSTEM_TABLES.role]: ['route'],
  [SYSTEM_TABLES.method]: ['route', 'graphql'],

  [SYSTEM_TABLES.guard]: ['guard'],
  [SYSTEM_TABLES.guardRule]: ['guard'],

  [SYSTEM_TABLES.fieldPermission]: ['fieldPermission', 'graphql'],

  [SYSTEM_TABLES.columnRule]: ['column-rule'],

  [SYSTEM_TABLES.setting]: ['setting', 'settingGraphql'],
  [SYSTEM_TABLES.storageConfig]: ['storage'],
  [SYSTEM_TABLES.oauthConfig]: ['oauth'],
  [SYSTEM_TABLES.websocket]: ['websocket'],
  [SYSTEM_TABLES.websocketEvent]: ['websocket'],
  [SYSTEM_TABLES.package]: ['package'],
  [SYSTEM_TABLES.flow]: ['flow'],
  [SYSTEM_TABLES.flowStep]: ['flow'],
  [SYSTEM_TABLES.folder]: ['folder'],
  [SYSTEM_TABLES.bootstrapScript]: ['bootstrap'],
  [SYSTEM_TABLES.menu]: ['menu', 'extension'],
  [SYSTEM_TABLES.extension]: ['extension'],
  [SYSTEM_TABLES.graphql]: ['graphql'],
};

export class CacheOrchestratorService implements LifecycleAware {
  private readonly logger = new Logger(`${COLOR}CacheOrchestrator${RESET}`);
  private readonly redisPubSubService: RedisPubSubService;
  private readonly instanceService: InstanceService;
  private readonly eventEmitter: EventEmitter2;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly routeCacheService: RouteCacheService;
  private readonly guardCacheBuilder: GuardCacheBuilder;
  private readonly flowCacheBuilder: FlowCacheBuilder;
  private readonly websocketCacheBuilder: WebsocketCacheBuilder;
  private readonly packageCacheService: PackageCacheService;
  private readonly settingCacheService: SettingCacheService;
  private readonly storageConfigCacheBuilder: StorageConfigCacheBuilder;
  private readonly oauthConfigCacheBuilder: OAuthConfigCacheBuilder;
  private readonly folderTreeCacheService: FolderTreeCacheService;
  private readonly fieldPermissionCacheBuilder: FieldPermissionCacheBuilder;
  private readonly columnRuleCacheBuilder: ColumnRuleCacheBuilder;
  private readonly gqlDefinitionCacheService: GqlDefinitionCacheService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly runtimeRegistryService?: RuntimeRegistryService;
  private readonly runtimeReloadAuditService?: RuntimeReloadAuditService;
  private readonly graphqlService: GraphqlService;
  private readonly bootstrapScriptService: BootstrapScriptService;
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;
  private readonly runtimeMetricsCollectorService?: RuntimeMetricsCollectorService;
  private readonly redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  private stepMap: Record<string, ReloadStep>;
  private messageHandler: ((channel: string, message: string) => void) | null =
    null;
  private readonly invalidationHandler: (
    payload: TCacheInvalidationPayload,
  ) => Promise<void>;

  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private debounceResolvers: Array<() => void> = [];
  private pendingPayload: TCacheInvalidationPayload | null = null;
  private reloadLock: Promise<void> | null = null;
  private reloadEventSequence = 0;
  private processedVersions: Set<string> = new Set();

  constructor(deps: {
    redisPubSubService: RedisPubSubService;
    instanceService: InstanceService;
    eventEmitter: EventEmitter2;
    metadataCacheService: MetadataCacheService;
    routeCacheService: RouteCacheService;
    guardCacheBuilder: GuardCacheBuilder;
    flowCacheBuilder: FlowCacheBuilder;
    websocketCacheBuilder: WebsocketCacheBuilder;
    packageCacheService: PackageCacheService;
    settingCacheService: SettingCacheService;
    storageConfigCacheBuilder: StorageConfigCacheBuilder;
    oauthConfigCacheBuilder: OAuthConfigCacheBuilder;
    folderTreeCacheService: FolderTreeCacheService;
    fieldPermissionCacheBuilder: FieldPermissionCacheBuilder;
    columnRuleCacheBuilder: ColumnRuleCacheBuilder;
    gqlDefinitionCacheService: GqlDefinitionCacheService;
    repoRegistryService: RepoRegistryService;
    runtimeRegistryService?: RuntimeRegistryService;
    runtimeReloadAuditService?: RuntimeReloadAuditService;
    graphqlService: GraphqlService;
    bootstrapScriptService: BootstrapScriptService;
    dynamicWebSocketGateway: DynamicWebSocketGateway;
    runtimeMetricsCollectorService?: RuntimeMetricsCollectorService;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    this.redisPubSubService = deps.redisPubSubService;
    this.instanceService = deps.instanceService;
    this.eventEmitter = deps.eventEmitter;
    this.metadataCacheService = deps.metadataCacheService;
    this.routeCacheService = deps.routeCacheService;
    this.guardCacheBuilder = deps.guardCacheBuilder;
    this.flowCacheBuilder = deps.flowCacheBuilder;
    this.websocketCacheBuilder = deps.websocketCacheBuilder;
    this.packageCacheService = deps.packageCacheService;
    this.settingCacheService = deps.settingCacheService;
    this.storageConfigCacheBuilder = deps.storageConfigCacheBuilder;
    this.oauthConfigCacheBuilder = deps.oauthConfigCacheBuilder;
    this.folderTreeCacheService = deps.folderTreeCacheService;
    this.fieldPermissionCacheBuilder = deps.fieldPermissionCacheBuilder;
    this.columnRuleCacheBuilder = deps.columnRuleCacheBuilder;
    this.gqlDefinitionCacheService = deps.gqlDefinitionCacheService;
    this.repoRegistryService = deps.repoRegistryService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.runtimeReloadAuditService = deps.runtimeReloadAuditService;
    this.graphqlService = deps.graphqlService;
    this.bootstrapScriptService = deps.bootstrapScriptService;
    this.dynamicWebSocketGateway = deps.dynamicWebSocketGateway;
    this.runtimeMetricsCollectorService = deps.runtimeMetricsCollectorService;
    this.redisRuntimeCacheStore = deps.redisRuntimeCacheStore;

    this.stepMap = {
      metadata: (p, options) => this.reloadMetadata(p, options?.sharedReplay),
      repoRegistry: () => this.reloadRepoRegistry(),
      route: (p, options) => this.reloadRoute(p, options?.sharedReplay),
      graphql: (p, options) => this.reloadGraphql(p, options?.sharedReplay),
      guard: (p, options) =>
        this.reloadSimple(this.guardCacheBuilder, p, options?.sharedReplay),
      flow: (p, options) =>
        this.reloadSimple(this.flowCacheBuilder, p, options?.sharedReplay),
      websocket: (p, options) =>
        this.reloadSimple(this.websocketCacheBuilder, p, options?.sharedReplay),
      package: (p, options) =>
        this.reloadSimple(this.packageCacheService, p, options?.sharedReplay),
      setting: (p, options) =>
        this.reloadSimple(this.settingCacheService, p, options?.sharedReplay),
      storage: (p, options) =>
        this.reloadSimple(
          this.storageConfigCacheBuilder,
          p,
          options?.sharedReplay,
        ),
      oauth: (p, options) =>
        this.reloadSimple(
          this.oauthConfigCacheBuilder,
          p,
          options?.sharedReplay,
        ),
      folder: (p, options) =>
        this.reloadSimple(
          this.folderTreeCacheService,
          p,
          options?.sharedReplay,
        ),
      menu: async () => undefined,
      extension: async () => undefined,
      fieldPermission: (p, options) =>
        this.reloadSimple(
          this.fieldPermissionCacheBuilder,
          p,
          options?.sharedReplay,
        ),
      'column-rule': (p, options) =>
        this.reloadSimple(
          this.columnRuleCacheBuilder,
          p,
          options?.sharedReplay,
        ),
      settingGraphql: async () => undefined,
      bootstrap: () => this.reloadBootstrapScripts(),
    };

    this.invalidationHandler = this.handleInvalidation.bind(this);
    deps.eventEmitter.on(CACHE_EVENTS.INVALIDATE, this.invalidationHandler);
  }

  async init() {
    this.subscribeToRedis();
  }

  onDestroy(): void {
    this.eventEmitter.off(CACHE_EVENTS.INVALIDATE, this.invalidationHandler);
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    const resolvers = this.debounceResolvers.splice(0);
    resolvers.forEach((resolve) => resolve());
    this.pendingPayload = null;
    this.messageHandler = null;
  }

  private handleInvalidation(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    return new Promise<void>((resolve) => {
      this.debounceResolvers.push(resolve);
      this.mergePayload(payload);
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(async () => {
        this.debounceTimer = null;
        const resolvers = this.debounceResolvers.splice(0);
        const merged = this.pendingPayload;
        this.pendingPayload = null;
        try {
          if (merged) {
            await this.executeChain(merged, true);
          }
        } catch (error) {
          this.logger.error('Invalidation chain failed:', error);
        }
        resolvers.forEach((r) => r());
      }, 50);
    });
  }

  private mergePayload(payload: TCacheInvalidationPayload): void {
    if (!this.pendingPayload) {
      this.pendingPayload = { ...payload };
      return;
    }
    if (this.pendingPayload.table !== payload.table) {
      const currentChain = RELOAD_CHAINS[this.pendingPayload.table] || [];
      const incomingChain = RELOAD_CHAINS[payload.table] || [];
      if (incomingChain.length > currentChain.length) {
        this.pendingPayload.table = payload.table;
      }
      this.pendingPayload.scope = 'full';
      this.pendingPayload.ids = undefined;
      this.pendingPayload.affectedTables = undefined;
      this.pendingPayload.tableRenames = undefined;
      return;
    }
    if (payload.scope === 'full' || this.pendingPayload.scope === 'full') {
      this.pendingPayload.scope = 'full';
      this.pendingPayload.ids = undefined;
      this.pendingPayload.affectedTables = undefined;
      this.pendingPayload.tableRenames = undefined;
      return;
    }
    const mergedIds = new Set([
      ...(this.pendingPayload.ids || []),
      ...(payload.ids || []),
    ]);
    const mergedTables = new Set([
      ...(this.pendingPayload.affectedTables || []),
      ...(payload.affectedTables || []),
    ]);
    const mergedRenames = [
      ...(this.pendingPayload.tableRenames || []),
      ...(payload.tableRenames || []),
    ];
    this.pendingPayload.ids = [...mergedIds];
    this.pendingPayload.affectedTables = mergedTables.size
      ? [...mergedTables]
      : undefined;
    this.pendingPayload.tableRenames = mergedRenames.length
      ? mergedRenames
      : undefined;
  }

  private async executeChain(
    payload: TCacheInvalidationPayload,
    publish: boolean,
  ): Promise<void> {
    const chain = RELOAD_CHAINS[payload.table];
    if (!chain) {
      this.logger.debug(`No reload chain for table: ${payload.table}`);
      return;
    }

    const sharedRuntimeCache = this.usesSharedRuntimeCache();
    const sharedReplay = !publish && sharedRuntimeCache;

    if (publish && !sharedRuntimeCache) {
      await this.publishSignal(payload);
    }

    const flow = this.resolveFlowName(chain);

    const reloadId = this.createReloadEventId(flow);
    const auditStartedAt = Date.now();
    let auditSteps: CacheReloadStepMetric[] = [];
    let elapsed = 0;
    let reloadFailed = false;
    const runReload = async () => {
      const start = Date.now();
      const startedAt = new Date(start).toISOString();
      const stepTimings: string[] = [];
      const steps: Array<{
        name: string;
        durationMs: number;
        status: 'success' | 'failed';
        error?: string;
      }> = [];
      let reloadError: unknown = null;
      const memoryMeta = {
        reloadId,
        flow,
        table: payload.table,
        scope: payload.scope,
        chain,
        sharedRuntimeCache,
        sharedReplay,
      };

      logMemory(this.logger, 'cache chain start', memoryMeta);

      const runStep = async (name: string, fn: () => Promise<void>) => {
        const s = Date.now();
        logMemory(this.logger, 'cache step start', {
          ...memoryMeta,
          step: name,
        });
        try {
          await fn();
          const durationMs = Date.now() - s;
          logMemory(this.logger, 'cache step done', {
            ...memoryMeta,
            step: name,
            durationMs,
          });
          steps.push({ name, durationMs, status: 'success' });
          return durationMs;
        } catch (error: any) {
          const durationMs = Date.now() - s;
          logMemory(this.logger, 'cache step failed', {
            ...memoryMeta,
            step: name,
            durationMs,
            error: error?.message || String(error),
          });
          steps.push({
            name,
            durationMs,
            status: 'failed',
            error: error?.message || String(error),
          });
          throw error;
        }
      };

      try {
        if (chain.includes('metadata')) {
          const durationMs = await runStep('metadata', () =>
            this.stepMap['metadata'](payload, { sharedReplay }),
          );
          stepTimings.push(`metadata:${durationMs}ms`);
        }

        const middleSteps = chain.filter(
          (s) => s !== 'metadata' && s !== 'graphql' && s !== 'settingGraphql',
        );
        if (middleSteps.length > 0) {
          const s = Date.now();
          await Promise.all(
            middleSteps.map(async (step) => {
              const fn = this.stepMap[step];
              if (fn) {
                await runStep(step, () => fn(payload, { sharedReplay }));
              }
            }),
          );
          stepTimings.push(`[${middleSteps.join('+')}]:${Date.now() - s}ms`);
        }

        if (chain.includes('graphql')) {
          const durationMs = await runStep('graphql', () =>
            this.stepMap['graphql'](payload, { sharedReplay }),
          );
          stepTimings.push(`graphql:${durationMs}ms`);
        }
      } catch (error) {
        reloadError = error;
        throw error;
      } finally {
        auditSteps = steps;
        const completedAt = new Date().toISOString();
        const durationMs = Date.now() - start;
        this.runtimeMetricsCollectorService?.recordCacheReload({
          reloadId,
          flow,
          table: payload.table,
          scope: payload.scope,
          status: reloadError ? 'failed' : 'success',
          durationMs,
          steps,
          startedAt,
          completedAt,
          error: reloadError
            ? reloadError instanceof Error
              ? reloadError.message
              : String(reloadError)
            : undefined,
        });
      }

      elapsed = Date.now() - start;
      this.logger.log(
        `${payload.scope === 'partial' ? 'Partial' : 'Full'} chain [${stepTimings.join(' → ')}] for ${payload.table} in ${elapsed}ms`,
      );
      logMemory(this.logger, 'cache chain done', {
        ...memoryMeta,
        durationMs: elapsed,
      });
    };
    await this.runWithReloadLock(async () => {
      await this.runtimeReloadAuditService?.markBuilding({
        reloadId,
        flow,
        table: payload.table,
        scope: payload.scope,
        action: payload.action,
        chain,
        payload,
        instanceId: this.instanceService.getInstanceId(),
      });
      this.notifyClients('pending', flow, chain, reloadId);

      try {
        if (this.runtimeMetricsCollectorService) {
          await this.runtimeMetricsCollectorService.runWithQueryContext(
            'cache',
            runReload,
          );
        } else {
          await runReload();
        }
        await this.commitRuntimeReloadTransaction(chain, payload);
        if (publish && sharedRuntimeCache) {
          await this.publishSignal(payload);
        }
        await this.runtimeReloadAuditService?.markActivated({
          reloadId,
          durationMs: Date.now() - auditStartedAt,
          steps: auditSteps,
        });
      } catch (error) {
        reloadFailed = true;
        await this.runtimeReloadAuditService?.markFailed({
          reloadId,
          durationMs: Date.now() - auditStartedAt,
          steps: auditSteps,
          error: this.formatReloadError(error),
        });
        throw error;
      } finally {
        if (!reloadFailed && elapsed < 500) {
          await new Promise((r) => setTimeout(r, 500 - elapsed));
        }
        this.notifyClients(
          reloadFailed ? 'failed' : 'done',
          flow,
          chain,
          reloadId,
        );
      }
    });
  }

  private async runWithReloadLock(run: () => Promise<void>): Promise<void> {
    const previous = this.reloadLock;
    const current = (previous ?? Promise.resolve())
      .catch(() => undefined)
      .then(run);
    this.reloadLock = current;
    try {
      await current;
    } finally {
      if (this.reloadLock === current) {
        this.reloadLock = null;
      }
    }
  }

  private async runBoundedReloadSteps<T>(
    items: T[],
    concurrency: number,
    run: (item: T) => Promise<void>,
  ): Promise<void> {
    let nextIndex = 0;
    const workerCount = Math.min(Math.max(1, concurrency), items.length);
    await Promise.all(
      Array.from({ length: workerCount }, async () => {
        while (nextIndex < items.length) {
          const item = items[nextIndex++];
          await run(item);
        }
      }),
    );
  }

  private resolveFlowName(chain: string[]): string {
    for (const step of FLOW_PRIORITY) {
      if (chain.includes(step)) return step;
    }
    return chain[0] ?? 'unknown';
  }

  private createReloadEventId(flow: string): string {
    this.reloadEventSequence =
      this.reloadEventSequence >= Number.MAX_SAFE_INTEGER
        ? 1
        : this.reloadEventSequence + 1;
    return `${this.instanceService.getInstanceId()}:${flow}:${Date.now()}:${this.reloadEventSequence}`;
  }

  private notifyClients(
    status: 'pending' | 'done' | 'failed',
    flow: string,
    steps?: string[],
    reloadId?: string,
  ): void {
    try {
      this.dynamicWebSocketGateway?.emitToNamespace?.(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        '$system:reload',
        {
          flow,
          status,
          steps,
          reloadId,
          instanceId: this.instanceService.getInstanceId(),
        },
      );
    } catch {}
  }

  private async reloadMetadata(
    payload: TCacheInvalidationPayload,
    sharedReplay = false,
  ): Promise<void> {
    if (sharedReplay) {
      await this.metadataCacheService.syncFromSharedCache();
      return;
    }
    if (
      payload.scope === 'partial' &&
      payload.ids?.length &&
      this.metadataCacheService.isLoaded()
    ) {
      await this.metadataCacheService.partialReload(payload);
    } else {
      await this.metadataCacheService.reload();
    }
  }

  private async reloadRepoRegistry(): Promise<void> {
    await this.repoRegistryService.rebuildFromMetadata(
      this.metadataCacheService,
    );
  }

  private async reloadRoute(
    payload: TCacheInvalidationPayload,
    sharedReplay = false,
  ): Promise<void> {
    if (sharedReplay) {
      await this.routeCacheService.syncFromSharedCache();
      return;
    }
    if (
      payload.scope === 'partial' &&
      this.routeCacheService.isLoaded() &&
      this.routeCacheService.supportsPartialReload()
    ) {
      await this.routeCacheService.partialReload(payload, false);
    } else {
      await this.routeCacheService.reload(false);
    }
  }

  private async reloadGraphql(
    payload: TCacheInvalidationPayload,
    sharedReplay = false,
  ): Promise<void> {
    if (sharedReplay) {
      await this.gqlDefinitionCacheService.syncFromSharedCache();
      return;
    }
    await this.gqlDefinitionCacheService.reload(false);
  }

  private async reloadSimple(
    cache: {
      reload: (publish?: boolean) => Promise<void>;
      partialReload?: (
        payload: TCacheInvalidationPayload,
        publish?: boolean,
      ) => Promise<void>;
      supportsPartialReload?: () => boolean;
      isLoaded?: () => boolean;
      syncFromSharedCache?: () => Promise<void>;
    },
    payload: TCacheInvalidationPayload,
    sharedReplay = false,
  ): Promise<void> {
    if (sharedReplay && cache.syncFromSharedCache) {
      await cache.syncFromSharedCache();
      return;
    }
    if (
      payload.scope === 'partial' &&
      payload.ids?.length &&
      cache.isLoaded?.() &&
      cache.supportsPartialReload?.() &&
      cache.partialReload
    ) {
      await cache.partialReload(payload, false);
      return;
    }
    await cache.reload(false);
  }

  private async reloadSettingGraphql(): Promise<void> {
    await this.graphqlService?.onSettingChanged?.();
  }

  private async reloadBootstrapScripts(): Promise<void> {
    if (!this.bootstrapScriptService) return;
    await this.bootstrapScriptService.reloadBootstrapScripts();
  }

  private getRuntimeCachePublishTarget(step: string): {
    identifier: RuntimeCacheIdentifier;
    service: RuntimeCacheViewSource;
  } | null {
    switch (step) {
      case 'metadata':
        return {
          identifier: CACHE_IDENTIFIERS.METADATA,
          service: this
            .metadataCacheService as unknown as RuntimeCacheViewSource,
        };
      case 'route':
        return {
          identifier: CACHE_IDENTIFIERS.ROUTE,
          service: this.routeCacheService as unknown as RuntimeCacheViewSource,
        };
      case 'guard':
        return {
          identifier: CACHE_IDENTIFIERS.GUARD,
          service: this.guardCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      case 'flow':
        return {
          identifier: CACHE_IDENTIFIERS.FLOW,
          service: this.flowCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      case 'websocket':
        return {
          identifier: CACHE_IDENTIFIERS.WEBSOCKET,
          service: this
            .websocketCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      case 'package':
        return {
          identifier: CACHE_IDENTIFIERS.PACKAGE,
          service: this
            .packageCacheService as unknown as RuntimeCacheViewSource,
        };
      case 'setting':
        return {
          identifier: CACHE_IDENTIFIERS.SETTING,
          service: this
            .settingCacheService as unknown as RuntimeCacheViewSource,
        };
      case 'storage':
        return {
          identifier: CACHE_IDENTIFIERS.STORAGE,
          service: this
            .storageConfigCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      case 'oauth':
        return {
          identifier: CACHE_IDENTIFIERS.OAUTH_CONFIG,
          service: this
            .oauthConfigCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      case 'folder':
        return {
          identifier: CACHE_IDENTIFIERS.FOLDER_TREE,
          service: this
            .folderTreeCacheService as unknown as RuntimeCacheViewSource,
        };
      case 'graphql':
        return {
          identifier: CACHE_IDENTIFIERS.GRAPHQL,
          service: this
            .gqlDefinitionCacheService as unknown as RuntimeCacheViewSource,
        };
      case 'fieldPermission':
        return {
          identifier: CACHE_IDENTIFIERS.FIELD_PERMISSION,
          service: this
            .fieldPermissionCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      case 'column-rule':
      case 'columnRule':
        return {
          identifier: CACHE_IDENTIFIERS.COLUMN_RULE,
          service: this
            .columnRuleCacheBuilder as unknown as RuntimeCacheViewSource,
        };
      default:
        return null;
    }
  }

  private async publishRuntimeCachesForSteps(steps: string[]): Promise<void> {
    if (!this.runtimeRegistryService) return;

    const published = new Set<RuntimeCacheIdentifier>();
    for (const step of steps) {
      const target = this.getRuntimeCachePublishTarget(step);
      if (!target || published.has(target.identifier)) continue;
      await this.runtimeRegistryService.publishFromCache(
        target.identifier,
        target.service,
      );
      published.add(target.identifier);
    }
  }

  private async stageRuntimeSnapshotsForSteps(
    steps: string[],
  ): Promise<RuntimeRegistrySnapshot[]> {
    if (!this.runtimeRegistryService?.stageSnapshotFromCache) return [];

    const staged: RuntimeRegistrySnapshot[] = [];
    const published = new Set<RuntimeCacheIdentifier>();
    for (const step of steps) {
      const target = this.getRuntimeCachePublishTarget(step);
      if (!target || published.has(target.identifier)) continue;
      staged.push(
        await this.runtimeRegistryService.stageSnapshotFromCache(
          target.identifier,
          target.service,
        ),
      );
      published.add(target.identifier);
    }
    return staged;
  }

  private async commitRuntimeReloadTransaction(
    steps: string[],
    payload?: TCacheInvalidationPayload,
  ): Promise<void> {
    if (
      !this.runtimeRegistryService?.stageSnapshotFromCache ||
      !this.runtimeRegistryService?.activateSnapshots
    ) {
      await this.publishRuntimeCachesForSteps(steps);
      await this.reloadRuntimeArtifactsAfterCommit(steps, payload);
      return;
    }

    const snapshots = await this.stageRuntimeSnapshotsForSteps(steps);
    this.runtimeRegistryService.activateSnapshots(snapshots);
    await this.reloadRuntimeArtifactsAfterCommit(steps, payload);
  }

  private async reloadRuntimeArtifactsAfterCommit(
    steps: string[],
    payload?: TCacheInvalidationPayload,
  ): Promise<void> {
    if (steps.includes('graphql')) {
      await this.graphqlService?.reloadSchema?.(payload);
    } else if (steps.includes('settingGraphql')) {
      await this.reloadSettingGraphql();
    }
  }

  private formatReloadError(error: unknown): string | undefined {
    if (!error) return undefined;
    return error instanceof Error ? error.message : String(error);
  }

  private createReloadTracker(
    flow: string,
    table: string,
    scope: TCacheInvalidationPayload['scope'],
    reloadId?: string,
  ) {
    const start = Date.now();
    const startedAt = new Date(start).toISOString();
    const steps: CacheReloadStepMetric[] = [];
    const runStep: CacheReloadStepRunner = async (name, fn) => {
      const stepStart = Date.now();
      try {
        await fn();
        steps.push({
          name,
          durationMs: Date.now() - stepStart,
          status: 'success',
        });
      } catch (error) {
        steps.push({
          name,
          durationMs: Date.now() - stepStart,
          status: 'failed',
          error: this.formatReloadError(error),
        });
        throw error;
      }
    };
    const finish = (error: unknown) => {
      this.runtimeMetricsCollectorService?.recordCacheReload({
        reloadId,
        flow,
        table,
        scope,
        status: error ? 'failed' : 'success',
        durationMs: Date.now() - start,
        steps,
        startedAt,
        completedAt: new Date().toISOString(),
        error: this.formatReloadError(error),
      });
    };
    return {
      runStep,
      finish,
      steps,
      elapsed: () => Date.now() - start,
    };
  }

  private async runTrackedAdminReload(input: {
    flow: string;
    table: string;
    steps: string[];
    logLabel: string;
    run: (runStep: CacheReloadStepRunner) => Promise<void>;
  }): Promise<void> {
    const reloadId = this.createReloadEventId(input.flow);
    const tracker = this.createReloadTracker(
      input.flow,
      input.table,
      'full',
      reloadId,
    );
    const runReload = async () => {
      let reloadError: unknown = null;
      await this.runtimeReloadAuditService?.markBuilding({
        reloadId,
        flow: input.flow,
        table: input.table,
        scope: 'full',
        action: 'reload',
        chain: input.steps,
        instanceId: this.instanceService.getInstanceId(),
      });
      this.notifyClients('pending', input.flow, input.steps, reloadId);
      try {
        await input.run(tracker.runStep);
        await this.commitRuntimeReloadTransaction(input.steps);
        await this.runtimeReloadAuditService?.markActivated({
          reloadId,
          durationMs: tracker.elapsed(),
          steps: tracker.steps,
        });
      } catch (error) {
        reloadError = error;
        this.logger.error(`${input.logLabel} failed:`, error);
        await this.runtimeReloadAuditService?.markFailed({
          reloadId,
          durationMs: tracker.elapsed(),
          steps: tracker.steps,
          error: this.formatReloadError(error),
        });
        throw error;
      } finally {
        tracker.finish(reloadError);
        this.notifyClients(
          reloadError ? 'failed' : 'done',
          input.flow,
          input.steps,
          reloadId,
        );
      }
      this.logger.log(`${input.logLabel}: ${tracker.elapsed()}ms`);
    };

    await this.runWithReloadLock(async () => {
      if (this.runtimeMetricsCollectorService) {
        await this.runtimeMetricsCollectorService.runWithQueryContext(
          'cache',
          runReload,
        );
        return;
      }
      await runReload();
    });
  }

  async reloadMetadataAndDeps(): Promise<void> {
    const steps = ['metadata', 'repoRegistry', 'route', 'graphql'];
    await this.runTrackedAdminReload({
      flow: 'metadata',
      table: 'enfyra_table',
      steps,
      logLabel: 'Admin reload metadata+deps',
      run: async (runStep) => {
        await runStep('metadata', () => this.metadataCacheService.reload());
        await runStep('repoRegistry', () => this.reloadRepoRegistry());
        await runStep('route', () => this.routeCacheService.reload(false));
        await runStep('graphql', () =>
          this.gqlDefinitionCacheService.reload(false),
        );
      },
    });
    await this.publishSignal({
      table: 'enfyra_table',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadRoutesOnly(): Promise<void> {
    const steps = ['route'];
    await this.runTrackedAdminReload({
      flow: 'route',
      table: 'enfyra_route',
      steps,
      logLabel: 'Admin reload routes',
      run: async (runStep) => {
        await runStep('route', () => this.routeCacheService.reload(false));
      },
    });
    await this.publishSignal({
      table: 'enfyra_route',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadGraphqlOnly(): Promise<void> {
    const steps = ['graphql'];
    await this.runTrackedAdminReload({
      flow: 'graphql',
      table: 'enfyra_graphql',
      steps,
      logLabel: 'Admin reload graphql',
      run: async (runStep) => {
        await runStep('graphql', () =>
          this.gqlDefinitionCacheService.reload(false),
        );
      },
    });
    await this.publishSignal({
      table: 'enfyra_graphql',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadGuardsOnly(): Promise<void> {
    const steps = ['guard'];
    await this.runTrackedAdminReload({
      flow: 'guard',
      table: 'enfyra_guard',
      steps,
      logLabel: 'Admin reload guards',
      run: async (runStep) => {
        await runStep('guard', () => this.guardCacheBuilder.reload(false));
      },
    });
    await this.publishSignal({
      table: 'enfyra_guard',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadAll(): Promise<void> {
    const payload: TCacheInvalidationPayload = {
      table: '__admin_reload_all',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    };
    if (!this.usesSharedRuntimeCache()) {
      await this.publishSignal(payload);
    }
    await this.reloadAllLocal(true, false);
    if (this.usesSharedRuntimeCache()) {
      await this.publishSignal(payload);
    }
  }

  private async reloadAllLocal(
    notify = true,
    sharedReplay = false,
  ): Promise<void> {
    const runReload = async () => {
      const start = Date.now();
      const startedAt = new Date(start).toISOString();
      const steps = [
        'metadata',
        'repoRegistry',
        'route',
        'guard',
        'flow',
        'websocket',
        'package',
        'setting',
        'storage',
        'oauth',
        'folder',
        'fieldPermission',
        'columnRule',
        'graphql',
      ];
      const stepMetrics: Array<{
        name: string;
        durationMs: number;
        status: 'success' | 'failed';
        error?: string;
      }> = [];
      let reloadError: unknown = null;
      const reloadId = this.createReloadEventId('all');
      const runStep = async (name: string, fn: () => Promise<void>) => {
        const s = Date.now();
        try {
          await fn();
          stepMetrics.push({
            name,
            durationMs: Date.now() - s,
            status: 'success',
          });
        } catch (error: any) {
          stepMetrics.push({
            name,
            durationMs: Date.now() - s,
            status: 'failed',
            error: error?.message || String(error),
          });
          throw error;
        }
      };
      try {
        await this.runtimeReloadAuditService?.markBuilding({
          reloadId,
          flow: 'all',
          table: '__admin_reload_all',
          scope: 'full',
          action: 'reload',
          chain: steps,
          instanceId: this.instanceService.getInstanceId(),
        });
        if (notify) this.notifyClients('pending', 'all', steps, reloadId);
        await runStep('metadata', () =>
          sharedReplay
            ? this.metadataCacheService.syncFromSharedCache()
            : this.metadataCacheService.reload(),
        );
        const builderSteps = [
          { name: 'repoRegistry', run: () => this.reloadRepoRegistry() },
          {
            name: 'route',
            run: () =>
              sharedReplay
                ? this.routeCacheService.syncFromSharedCache()
                : this.routeCacheService.reload(false),
          },
          {
            name: 'guard',
            run: () =>
              sharedReplay
                ? this.guardCacheBuilder.syncFromSharedCache()
                : this.guardCacheBuilder.reload(false),
          },
          {
            name: 'flow',
            run: () =>
              sharedReplay
                ? this.flowCacheBuilder.syncFromSharedCache()
                : this.flowCacheBuilder.reload(false),
          },
          {
            name: 'websocket',
            run: () =>
              sharedReplay
                ? this.websocketCacheBuilder.syncFromSharedCache()
                : this.websocketCacheBuilder.reload(false),
          },
          {
            name: 'package',
            run: () =>
              sharedReplay
                ? this.packageCacheService.syncFromSharedCache()
                : this.packageCacheService.reload(false),
          },
          {
            name: 'setting',
            run: () =>
              sharedReplay
                ? this.settingCacheService.syncFromSharedCache()
                : this.settingCacheService.reload(false),
          },
          {
            name: 'storage',
            run: () =>
              sharedReplay
                ? this.storageConfigCacheBuilder.syncFromSharedCache()
                : this.storageConfigCacheBuilder.reload(false),
          },
          {
            name: 'oauth',
            run: () =>
              sharedReplay
                ? this.oauthConfigCacheBuilder.syncFromSharedCache()
                : this.oauthConfigCacheBuilder.reload(false),
          },
          {
            name: 'folder',
            run: () =>
              sharedReplay
                ? this.folderTreeCacheService.syncFromSharedCache()
                : this.folderTreeCacheService.reload(false),
          },
          {
            name: 'fieldPermission',
            run: () =>
              sharedReplay
                ? this.fieldPermissionCacheBuilder.syncFromSharedCache()
                : this.fieldPermissionCacheBuilder.reload(false),
          },
          {
            name: 'columnRule',
            run: () =>
              sharedReplay
                ? this.columnRuleCacheBuilder.syncFromSharedCache()
                : this.columnRuleCacheBuilder.reload(false),
          },
          {
            name: 'graphql',
            run: () =>
              sharedReplay
                ? this.gqlDefinitionCacheService.syncFromSharedCache()
                : this.gqlDefinitionCacheService.reload(false),
          },
        ];
        await this.runBoundedReloadSteps(
          builderSteps,
          FULL_RELOAD_BUILDER_CONCURRENCY,
          (step) => runStep(step.name, step.run),
        );
        await this.commitRuntimeReloadTransaction(steps);
        await this.runtimeReloadAuditService?.markActivated({
          reloadId,
          durationMs: Date.now() - start,
          steps: stepMetrics,
        });
      } catch (error) {
        reloadError = error;
        this.logger.error('Admin reload ALL failed:', error);
        await this.runtimeReloadAuditService?.markFailed({
          reloadId,
          durationMs: Date.now() - start,
          steps: stepMetrics,
          error: this.formatReloadError(error),
        });
        throw error;
      } finally {
        if (notify) {
          if (!reloadError) {
            const elapsed = Date.now() - start;
            if (elapsed < 200) {
              await new Promise((r) => setTimeout(r, 200 - elapsed));
            }
          }
          this.notifyClients(
            reloadError ? 'failed' : 'done',
            'all',
            steps,
            reloadId,
          );
        }
        this.runtimeMetricsCollectorService?.recordCacheReload({
          reloadId,
          flow: 'all',
          table: '__admin_reload_all',
          scope: 'full',
          status: reloadError ? 'failed' : 'success',
          durationMs: Date.now() - start,
          steps: stepMetrics,
          startedAt,
          completedAt: new Date().toISOString(),
          error: reloadError
            ? reloadError instanceof Error
              ? reloadError.message
              : String(reloadError)
            : undefined,
        });
      }
      this.logger.log(`Admin reload ALL: ${Date.now() - start}ms`);
    };
    await this.runWithReloadLock(async () => {
      if (this.runtimeMetricsCollectorService) {
        await this.runtimeMetricsCollectorService.runWithQueryContext(
          'cache',
          runReload,
        );
        return;
      }
      await runReload();
    });
  }

  private subscribeToRedis(): void {
    if (this.messageHandler) return;

    this.messageHandler = async (channel: string, message: string) => {
      if (this.redisPubSubService.isChannelForBase(channel, SYNC_CHANNEL)) {
        try {
          const signal = JSON.parse(message);
          if (signal.instanceId === this.instanceService.getInstanceId()) {
            return;
          }
          const version = `${signal.instanceId}:${signal.timestamp}:${signal.payload?.table}:${signal.payload?.scope || 'full'}:${signal.payload?.ids?.join(',') || 'all'}`;
          if (this.processedVersions.has(version)) {
            this.logger.debug(
              `Skipping duplicate/out-of-order signal: ${version.slice(0, 40)}...`,
            );
            return;
          }
          this.processedVersions.add(version);
          if (this.processedVersions.size > 1000) {
            const first = this.processedVersions.values().next().value!;
            this.processedVersions.delete(first);
          }
          this.logger.log(
            `Redis signal from ${signal.instanceId.slice(0, 8)}: ${signal.payload?.table} (${signal.payload?.scope || 'full'})`,
          );
          if (signal.payload?.table === '__admin_reload_all') {
            await this.reloadAllLocal(false, this.usesSharedRuntimeCache());
          } else {
            await this.executeChain(signal.payload, false);
          }
        } catch (error) {
          this.logger.error('Failed to process Redis signal:', error);
        }
      }
    };

    this.redisPubSubService.subscribeWithHandler(
      SYNC_CHANNEL,
      this.messageHandler,
    );
  }

  private async publishSignal(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    try {
      const timestamp = Date.now();
      const version = `${this.instanceService.getInstanceId()}:${timestamp}:${payload.table}:${payload.scope || 'full'}:${payload.ids?.join(',') || 'all'}`;
      this.processedVersions.add(version);
      if (this.processedVersions.size > 1000) {
        const first = this.processedVersions.values().next().value!;
        this.processedVersions.delete(first);
      }
      await this.redisPubSubService.publish(
        SYNC_CHANNEL,
        JSON.stringify({
          instanceId: this.instanceService.getInstanceId(),
          type: 'RELOAD_SIGNAL',
          timestamp,
          payload,
        }),
      );
    } catch (error) {
      this.logger.error('Failed to publish signal:', error);
    }
  }

  private usesSharedRuntimeCache(): boolean {
    return this.redisRuntimeCacheStore?.isEnabled() === true;
  }
}
