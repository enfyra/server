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
import { SharedHelperCacheService } from './shared-helper-cache.service';
import { SettingCacheService } from './setting-cache.service';
import { StorageConfigCacheBuilder } from './storage-config-cache-builder.service';
import { OAuthConfigCacheBuilder } from './oauth-config-cache-builder.service';
import { CorsOriginCacheBuilder } from './cors-origin-cache-builder.service';
import { AuthHeaderCacheBuilder } from './auth-header-cache-builder.service';
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
import { LifecycleAware } from '../../../shared/interfaces/lifecycle-aware.interface';
import type {
  RuntimeCacheIdentifier,
  RuntimeRegistrySnapshot,
} from '../types/runtime-registry.types';
import { CACHE_ORCHESTRATOR_DEFAULTS } from '../config/cache-orchestrator.config';

const COLOR = '\x1b[33m';
const RESET = '\x1b[0m';
const SYNC_CHANNEL = 'enfyra:cache-orchestrator-sync';
const FLOW_PRIORITY = [
  'metadata',
  'helper',
  'route',
  'fieldPermission',
  'setting',
  'authHeader',
  'guard',
  'flow',
  'websocket',
  'storage',
  'oauth',
  'folder',
  'package',
  'menu',
  'extension',
  'graphql',
  'settingGraphql',
  'repoRegistry',
];

type ReloadStepName =
  | 'metadata'
  | 'repoRegistry'
  | 'route'
  | 'graphql'
  | 'guard'
  | 'flow'
  | 'websocket'
  | 'package'
  | 'helper'
  | 'setting'
  | 'authHeader'
  | 'storage'
  | 'oauth'
  | 'corsOrigin'
  | 'folder'
  | 'menu'
  | 'extension'
  | 'fieldPermission'
  | 'column-rule'
  | 'settingGraphql';
type CacheBuilderReloadStepName = Exclude<
  ReloadStepName,
  'settingGraphql' | 'menu' | 'extension'
>;
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

const CLIENT_NOTIFICATION_STEPS = new Set<ReloadStepName>([
  'menu',
  'extension',
]);
const RUNTIME_ARTIFACT_STEPS = new Set<ReloadStepName>([
  'graphql',
  'settingGraphql',
]);

function isCacheBuilderReloadStep(
  step: ReloadStepName,
): step is CacheBuilderReloadStepName {
  return !CLIENT_NOTIFICATION_STEPS.has(step) && step !== 'settingGraphql';
}

export const RELOAD_CHAINS: Record<string, ReloadStepName[]> = {
  [SYSTEM_TABLES.table]: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
    'guard',
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

  [SYSTEM_TABLES.route]: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'guard',
  ],
  [SYSTEM_TABLES.routeMethodConfig]: ['route', 'guard'],
  [SYSTEM_TABLES.routeMethodConfigFileField]: ['route'],
  [SYSTEM_TABLES.preHook]: ['route'],
  [SYSTEM_TABLES.postHook]: ['route'],
  [SYSTEM_TABLES.routeHandler]: ['route'],
  [SYSTEM_TABLES.routePermission]: ['route'],
  [SYSTEM_TABLES.role]: ['route'],
  [SYSTEM_TABLES.method]: ['metadata', 'repoRegistry', 'route'],

  [SYSTEM_TABLES.guard]: ['guard'],
  [SYSTEM_TABLES.guardRule]: ['guard'],

  [SYSTEM_TABLES.fieldPermission]: ['fieldPermission', 'graphql'],

  [SYSTEM_TABLES.columnRule]: ['column-rule'],

  [SYSTEM_TABLES.setting]: ['setting', 'settingGraphql'],
  [SYSTEM_TABLES.authHeader]: ['authHeader'],
  [SYSTEM_TABLES.storageConfig]: ['storage'],
  [SYSTEM_TABLES.oauthConfig]: ['oauth'],
  [SYSTEM_TABLES.corsOrigin]: ['corsOrigin'],
  [SYSTEM_TABLES.websocket]: ['websocket'],
  [SYSTEM_TABLES.websocketEvent]: ['websocket'],
  [SYSTEM_TABLES.package]: ['package'],
  [SYSTEM_TABLES.helper]: ['helper'],
  [SYSTEM_TABLES.flow]: ['flow'],
  [SYSTEM_TABLES.flowStep]: ['flow'],
  [SYSTEM_TABLES.flowTrigger]: ['flow'],
  [SYSTEM_TABLES.folder]: ['folder'],
  [SYSTEM_TABLES.menu]: ['menu', 'extension'],
  [SYSTEM_TABLES.menuPermission]: ['menu'],
  [SYSTEM_TABLES.extension]: ['extension'],
  [SYSTEM_TABLES.graphql]: ['graphql'],
  [SYSTEM_TABLES.graphqlOperation]: ['graphql'],
  [SYSTEM_TABLES.graphqlPermission]: ['graphql'],
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
  private readonly sharedHelperCacheService: SharedHelperCacheService;
  private readonly settingCacheService: SettingCacheService;
  private readonly authHeaderCacheBuilder: AuthHeaderCacheBuilder;
  private readonly storageConfigCacheBuilder: StorageConfigCacheBuilder;
  private readonly oauthConfigCacheBuilder: OAuthConfigCacheBuilder;
  private readonly corsOriginCacheBuilder: CorsOriginCacheBuilder;
  private readonly folderTreeCacheService: FolderTreeCacheService;
  private readonly fieldPermissionCacheBuilder: FieldPermissionCacheBuilder;
  private readonly columnRuleCacheBuilder: ColumnRuleCacheBuilder;
  private readonly gqlDefinitionCacheService: GqlDefinitionCacheService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly runtimeRegistryService: RuntimeRegistryService | undefined;
  private readonly runtimeReloadAuditService: RuntimeReloadAuditService | undefined;
  private readonly graphqlService: GraphqlService;
  private readonly dynamicWebSocketGateway: DynamicWebSocketGateway;
  private readonly runtimeMetricsCollectorService: RuntimeMetricsCollectorService | undefined;
  private readonly redisRuntimeCacheStore: RedisRuntimeCacheStore | undefined;
  private stepMap: Record<CacheBuilderReloadStepName, ReloadStep>;
  private messageHandler: ((channel: string, message: string) => void) | null =
    null;
  private readonly invalidationHandler: (
    payload: TCacheInvalidationPayload,
  ) => Promise<void>;

  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private debounceResolvers: Array<{
    table: string;
    resolve: () => void;
    reject: (err: any) => void;
  }> = [];
  private pendingPayloads = new Map<string, TCacheInvalidationPayload>();
  private reloadLock: Promise<void> | null = null;
  private stagedRepoAliases: Map<string, string> | undefined;
  private reloadEventSequence = 0;
  private processedVersions: Set<string> = new Set();
  private signalRetryCounts: Map<string, number> = new Map();
  private signalFirstFailures: Map<string, number> = new Map();
  private appliedSharedGeneration: string | null = null;
  private reconcilingSharedGeneration = false;
  private generationReconciler: ReturnType<typeof setInterval> | null = null;

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
    sharedHelperCacheService: SharedHelperCacheService;
    settingCacheService: SettingCacheService;
    authHeaderCacheBuilder: AuthHeaderCacheBuilder;
    storageConfigCacheBuilder: StorageConfigCacheBuilder;
    oauthConfigCacheBuilder: OAuthConfigCacheBuilder;
    corsOriginCacheBuilder: CorsOriginCacheBuilder;
    folderTreeCacheService: FolderTreeCacheService;
    fieldPermissionCacheBuilder: FieldPermissionCacheBuilder;
    columnRuleCacheBuilder: ColumnRuleCacheBuilder;
    gqlDefinitionCacheService: GqlDefinitionCacheService;
    repoRegistryService: RepoRegistryService;
    runtimeRegistryService?: RuntimeRegistryService;
    runtimeReloadAuditService?: RuntimeReloadAuditService;
    graphqlService: GraphqlService;
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
    this.sharedHelperCacheService = deps.sharedHelperCacheService;
    this.settingCacheService = deps.settingCacheService;
    this.authHeaderCacheBuilder = deps.authHeaderCacheBuilder;
    this.storageConfigCacheBuilder = deps.storageConfigCacheBuilder;
    this.oauthConfigCacheBuilder = deps.oauthConfigCacheBuilder;
    this.corsOriginCacheBuilder = deps.corsOriginCacheBuilder;
    this.folderTreeCacheService = deps.folderTreeCacheService;
    this.fieldPermissionCacheBuilder = deps.fieldPermissionCacheBuilder;
    this.columnRuleCacheBuilder = deps.columnRuleCacheBuilder;
    this.gqlDefinitionCacheService = deps.gqlDefinitionCacheService;
    this.repoRegistryService = deps.repoRegistryService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.runtimeReloadAuditService = deps.runtimeReloadAuditService;
    this.graphqlService = deps.graphqlService;
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
      helper: (p, options) =>
        this.reloadSimple(
          this.sharedHelperCacheService,
          p,
          options?.sharedReplay,
        ),
      setting: (p, options) =>
        this.reloadSimple(this.settingCacheService, p, options?.sharedReplay),
      authHeader: (p, options) =>
        this.reloadSimple(
          this.authHeaderCacheBuilder,
          p,
          options?.sharedReplay,
        ),
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
      corsOrigin: (p, options) =>
        this.reloadSimple(
          this.corsOriginCacheBuilder,
          p,
          options?.sharedReplay,
        ),
      folder: (p, options) =>
        this.reloadSimple(
          this.folderTreeCacheService,
          p,
          options?.sharedReplay,
        ),
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
    };

    this.invalidationHandler = this.handleInvalidation.bind(this);
    deps.eventEmitter.on(CACHE_EVENTS.INVALIDATE, this.invalidationHandler);
  }

  async init() {
    this.subscribeToRedis();
    this.startSharedGenerationReconcile();
  }

  onDestroy(): void {
    this.eventEmitter.off(CACHE_EVENTS.INVALIDATE, this.invalidationHandler);
    if (this.generationReconciler) {
      clearInterval(this.generationReconciler);
      this.generationReconciler = null;
    }
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    const resolvers = this.debounceResolvers.splice(0);
    resolvers.forEach((r) => r.resolve());
    this.pendingPayloads.clear();
    this.messageHandler = null;
  }

  private handleInvalidation(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.debounceResolvers.push({ table: payload.table, resolve, reject });
      this.mergePayload(payload);
      if (this.debounceTimer) clearTimeout(this.debounceTimer);
      this.debounceTimer = setTimeout(async () => {
        this.debounceTimer = null;
        const resolvers = this.debounceResolvers.splice(0);
        const payloads = [...this.pendingPayloads.values()];
        this.pendingPayloads.clear();
        for (const merged of payloads) {
          const waiting = resolvers.filter((entry) => entry.table === merged.table);
          try {
            await this.executeChain(merged, true);
            waiting.forEach((entry) => entry.resolve());
          } catch (error) {
            this.logger.error('Invalidation chain failed:', error);
            waiting.forEach((entry) => merged.critical ? entry.reject(error) : entry.resolve());
          }
        }
      }, CACHE_ORCHESTRATOR_DEFAULTS.invalidationDebounceMs);
    });
  }

  private mergePayload(payload: TCacheInvalidationPayload): void {
    const pending = this.pendingPayloads.get(payload.table);
    if (!pending) {
      this.pendingPayloads.set(payload.table, { ...payload });
      return;
    }
    if (!pending.critical && payload.critical !== undefined) {
      pending.critical = payload.critical;
    }
    pending.timestamp = Math.max(pending.timestamp, payload.timestamp);
    const tables = new Set([...(pending.affectedTables ?? []), ...(payload.affectedTables ?? [])]);
    if (tables.size) pending.affectedTables = [...tables];
    else delete pending.affectedTables;
    const renames = [...(pending.tableRenames ?? []), ...(payload.tableRenames ?? [])];
    if (renames.length) pending.tableRenames = renames;
    else delete pending.tableRenames;
    if (payload.scope === 'full' || pending.scope === 'full') {
      pending.scope = 'full';
      delete pending.ids;
      return;
    }
    pending.ids = [...new Set([...(pending.ids ?? []), ...(payload.ids ?? [])])];
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
          (step): step is Exclude<CacheBuilderReloadStepName, 'metadata'> =>
            step !== 'metadata' && isCacheBuilderReloadStep(step),
        );
        if (middleSteps.length > 0) {
          const s = Date.now();
          const results = await Promise.allSettled(
            middleSteps.map(async (step) => {
              const fn = this.stepMap[step];
              if (fn) {
                await runStep(step, () => fn(payload, { sharedReplay }));
              }
            }),
          );
          const failure = results.find((result) => result.status === 'rejected');
          if (failure?.status === 'rejected') throw failure.reason;
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
          ...this.formatReloadError(reloadError),
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
        await this.commitRuntimeReloadTransaction(
          chain,
          payload,
          publish && sharedRuntimeCache ? payload : undefined,
        );
        await this.runtimeReloadAuditService?.markActivated({
          reloadId,
          durationMs: Date.now() - auditStartedAt,
          steps: auditSteps,
        });
        await this.markSharedGenerationApplied();
      } catch (error) {
        reloadFailed = true;
        await this.runtimeReloadAuditService?.markFailed({
          reloadId,
          durationMs: Date.now() - auditStartedAt,
          steps: auditSteps,
          ...this.formatReloadError(error),
        });
        throw error;
      } finally {
        if (
          !reloadFailed &&
          elapsed < CACHE_ORCHESTRATOR_DEFAULTS.minimumClientReloadStatusMs
        ) {
          await new Promise((r) =>
            setTimeout(
              r,
              CACHE_ORCHESTRATOR_DEFAULTS.minimumClientReloadStatusMs - elapsed,
            ),
          );
        }
        this.notifyClients(
          reloadFailed ? 'failed' : 'done',
          flow,
          chain,
          reloadId,
        );
      }
    }, sharedReplay);
  }

  private async runWithReloadLock(run: () => Promise<void>, sharedReplay = false): Promise<void> {
    const execute = async () => {
      this.stagedRepoAliases = undefined;
      try { await run(); } finally { this.stagedRepoAliases = undefined; }
    };
    const previous = this.reloadLock;
    const current = (previous ?? Promise.resolve())
      .catch(() => undefined)
      .then(() => this.redisRuntimeCacheStore?.isEnabled()
        ? sharedReplay
          ? this.redisRuntimeCacheStore.runWithCommittedSnapshots(execute)
          : this.redisRuntimeCacheStore.runWithPublication(execute)
        : execute());
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
    const pending = items.values();
    const workerCount = Math.min(Math.max(1, concurrency), items.length);
    const results = await Promise.allSettled(
      Array.from({ length: workerCount }, async () => {
        while (true) {
          const next = pending.next();
          if (next.done) return;
          await run(next.value);
        }
      }),
    );
    const failure = results.find((result) => result.status === 'rejected');
    if (failure?.status === 'rejected') throw failure.reason;
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
    this.stagedRepoAliases = await this.repoRegistryService.stageFromMetadata(
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
      case 'helper':
        return {
          identifier: CACHE_IDENTIFIERS.HELPER,
          service: this.sharedHelperCacheService,
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
      case 'authHeader':
        return {
          identifier: CACHE_IDENTIFIERS.AUTH_HEADER,
          service: this
            .authHeaderCacheBuilder as unknown as RuntimeCacheViewSource,
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
      case 'corsOrigin':
        return {
          identifier: CACHE_IDENTIFIERS.CORS_ORIGIN,
          service: this
            .corsOriginCacheBuilder as unknown as RuntimeCacheViewSource,
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
    signalPayload?: TCacheInvalidationPayload,
  ): Promise<void> {
    if (
      !this.runtimeRegistryService?.stageSnapshotFromCache ||
      !this.runtimeRegistryService?.activateSnapshots
    ) {
      await this.redisRuntimeCacheStore?.commitPublication();
      this.activateRepoAliases();
      await this.publishRuntimeCachesForSteps(steps);
      if (signalPayload) await this.publishSignal(signalPayload);
      await this.reloadRuntimeArtifactsAfterCommit(steps, payload);
      return;
    }

    const snapshots = await this.stageRuntimeSnapshotsForSteps(steps);
    await this.redisRuntimeCacheStore?.commitPublication();
    this.activateRepoAliases();
    this.runtimeRegistryService.activateSnapshots(snapshots);
    if (signalPayload) await this.publishSignal(signalPayload);
    await this.reloadRuntimeArtifactsAfterCommit(steps, payload);
  }

  private activateRepoAliases(): void {
    if (!this.stagedRepoAliases) return;
    this.repoRegistryService.activateAliases(this.stagedRepoAliases);
    this.stagedRepoAliases = undefined;
  }

  private async reloadRuntimeArtifactsAfterCommit(
    steps: string[],
    payload?: TCacheInvalidationPayload,
  ): Promise<void> {
    if (RUNTIME_ARTIFACT_STEPS.has('graphql') && steps.includes('graphql')) {
      await this.graphqlService?.reloadSchema?.(payload);
    } else if (
      RUNTIME_ARTIFACT_STEPS.has('settingGraphql') &&
      steps.includes('settingGraphql')
    ) {
      await this.reloadSettingGraphql();
    }
  }

  private formatReloadError(error: unknown): Pick<CacheReloadStepMetric, 'error'> {
    return error
      ? { error: error instanceof Error ? error.message : String(error) }
      : {};
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
          ...this.formatReloadError(error),
        });
        throw error;
      }
    };
    const finish = (error: unknown) => {
      this.runtimeMetricsCollectorService?.recordCacheReload({
        ...(reloadId !== undefined ? { reloadId } : {}),
        flow,
        table,
        scope,
        status: error ? 'failed' : 'success',
        durationMs: Date.now() - start,
        steps,
        startedAt,
        completedAt: new Date().toISOString(),
        ...this.formatReloadError(error),
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
        await this.commitRuntimeReloadTransaction(input.steps, undefined, {
          table: input.table,
          action: 'reload',
          scope: 'full',
          timestamp: Date.now(),
        });
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
          ...this.formatReloadError(error),
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
    const steps = ['metadata', 'repoRegistry', 'route', 'guard', 'graphql'];
    await this.runTrackedAdminReload({
      flow: 'metadata',
      table: 'enfyra_table',
      steps,
      logLabel: 'Admin reload metadata+deps',
      run: async (runStep) => {
        await runStep('metadata', () => this.metadataCacheService.reload());
        await runStep('repoRegistry', () => this.reloadRepoRegistry());
        await runStep('route', () => this.routeCacheService.reload(false));
        await runStep('guard', () => this.guardCacheBuilder.reload(false));
        await runStep('graphql', () =>
          this.gqlDefinitionCacheService.reload(false),
        );
      },
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
    await this.reloadAllLocal(true, false, this.usesSharedRuntimeCache() ? payload : undefined);
  }

  private async reloadAllLocal(
    notify = true,
    sharedReplay = false,
    signalPayload?: TCacheInvalidationPayload,
  ): Promise<void> {
    const runReload = async () => {
      const start = Date.now();
      const startedAt = new Date(start).toISOString();
      const steps = [
        'metadata',
        'helper',
        'repoRegistry',
        'route',
        'guard',
        'flow',
        'websocket',
        'package',
        'setting',
        'authHeader',
        'storage',
        'oauth',
        'corsOrigin',
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
        await runStep('helper', () =>
          sharedReplay
            ? this.sharedHelperCacheService.syncFromSharedCache()
            : this.sharedHelperCacheService.reload(false),
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
            name: 'authHeader',
            run: () =>
              sharedReplay
                ? this.authHeaderCacheBuilder.syncFromSharedCache()
                : this.authHeaderCacheBuilder.reload(false),
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
            name: 'corsOrigin',
            run: () =>
              sharedReplay
                ? this.corsOriginCacheBuilder.syncFromSharedCache()
                : this.corsOriginCacheBuilder.reload(false),
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
          CACHE_ORCHESTRATOR_DEFAULTS.fullReloadBuilderConcurrency,
          (step) => runStep(step.name, step.run),
        );
        await this.commitRuntimeReloadTransaction(steps, undefined, signalPayload);
        await this.runtimeReloadAuditService?.markActivated({
          reloadId,
          durationMs: Date.now() - start,
          steps: stepMetrics,
        });
        await this.markSharedGenerationApplied();
      } catch (error) {
        reloadError = error;
        this.logger.error('Admin reload ALL failed:', error);
        await this.runtimeReloadAuditService?.markFailed({
          reloadId,
          durationMs: Date.now() - start,
          steps: stepMetrics,
          ...this.formatReloadError(error),
        });
        throw error;
      } finally {
        if (notify) {
          if (!reloadError) {
            const elapsed = Date.now() - start;
            if (
              elapsed < CACHE_ORCHESTRATOR_DEFAULTS.minimumFullReloadStatusMs
            ) {
              await new Promise((r) =>
                setTimeout(
                  r,
                  CACHE_ORCHESTRATOR_DEFAULTS.minimumFullReloadStatusMs -
                    elapsed,
                ),
              );
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
          ...this.formatReloadError(reloadError),
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
    }, sharedReplay);
  }

  private subscribeToRedis(): void {
    if (this.messageHandler) return;

    this.messageHandler = async (channel: string, message: string) => {
      if (this.redisPubSubService.isChannelForBase(channel, SYNC_CHANNEL)) {
        let version: string | null = null;
        try {
          const signal = JSON.parse(message);
          if (signal.instanceId === this.instanceService.getInstanceId()) {
            return;
          }
          version = `${signal.instanceId}:${signal.timestamp}:${signal.payload?.table}:${signal.payload?.scope || 'full'}:${signal.payload?.ids?.join(',') || 'all'}`;
          if (this.processedVersions.has(version)) {
            this.logger.debug(
              `Skipping duplicate/out-of-order signal: ${version.slice(0, 40)}...`,
            );
            return;
          }
          this.logger.log(
            `Redis signal from ${signal.instanceId.slice(0, 8)}: ${signal.payload?.table} (${signal.payload?.scope || 'full'})`,
          );
          await this.eventEmitter.emitAsync(
            CACHE_EVENTS.SYNC_INVALIDATE,
            signal.payload,
          );
          if (this.usesSharedRuntimeCache()) {
            await this.reloadAllLocal(false, true);
            const clientSteps = RELOAD_CHAINS[signal.payload?.table]?.filter(
              step => step === 'menu' || step === 'extension',
            );
            if (clientSteps?.length) {
              this.notifyClients('done', this.resolveFlowName(clientSteps), clientSteps);
            }
          } else if (signal.payload?.table === '__admin_reload_all') {
            await this.reloadAllLocal(false, false);
          } else {
            await this.executeChain(signal.payload, false);
          }
          this.signalRetryCounts.delete(version);
          this.signalFirstFailures.delete(version);
          this.processedVersions.add(version);
          if (this.processedVersions.size > 1000) {
            const first = this.processedVersions.values().next().value!;
            this.processedVersions.delete(first);
          }
        } catch (error) {
          this.logger.error('Failed to process Redis signal:', error);
          if (!version) return;
          const retries = (this.signalRetryCounts.get(version) ?? 0) + 1;
          const firstFailure =
            this.signalFirstFailures.get(version) ?? Date.now();
          this.signalFirstFailures.set(version, firstFailure);
          const withinBudget =
            Date.now() - firstFailure <=
            CACHE_ORCHESTRATOR_DEFAULTS.signalRetryBudgetMs;
          if (
            retries <= CACHE_ORCHESTRATOR_DEFAULTS.signalRetryMaxAttempts ||
            withinBudget
          ) {
            this.signalRetryCounts.set(version, retries);
            setTimeout(
              () => this.messageHandler?.(channel, message),
              Math.min(
                CACHE_ORCHESTRATOR_DEFAULTS.signalRetryBaseDelayMs * retries,
                CACHE_ORCHESTRATOR_DEFAULTS.signalRetryMaxDelayMs,
              ),
            );
          } else {
            this.logger.error(
              `Giving up on Redis reload signal after ${CACHE_ORCHESTRATOR_DEFAULTS.signalRetryBudgetMs}ms: ${version.slice(0, 60)}`,
            );
            this.signalFirstFailures.delete(version);
            this.signalRetryCounts.delete(version);
          }
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
    const timestamp = Date.now();
    const version = `${this.instanceService.getInstanceId()}:${timestamp}:${payload.table}:${payload.scope || 'full'}:${payload.ids?.join(',') || 'all'}`;
    this.processedVersions.add(version);
    if (this.processedVersions.size > 1000) {
      const first = this.processedVersions.values().next().value!;
      this.processedVersions.delete(first);
    }
    try {
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
      if (payload.critical) {
        throw new Error(
          `Critical schema activation signal failed to publish: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
      this.logger.error('Failed to publish signal:', error);
    }
  }

  private usesSharedRuntimeCache(): boolean {
    return this.redisRuntimeCacheStore?.isEnabled() === true;
  }

  private startSharedGenerationReconcile(): void {
    if (!this.usesSharedRuntimeCache() || this.generationReconciler) return;
    const intervalMs = CACHE_ORCHESTRATOR_DEFAULTS.sharedGenerationReconcileMs;
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) return;
    this.generationReconciler = setInterval(() => {
      void this.reconcileSharedGeneration();
    }, intervalMs);
    this.generationReconciler.unref?.();
  }

  private async markSharedGenerationApplied(): Promise<void> {
    const store = this.redisRuntimeCacheStore;
    if (typeof store?.readCommittedGeneration !== 'function') return;
    try {
      const generation = await store.readCommittedGeneration();
      if (generation) this.appliedSharedGeneration = generation;
    } catch {
      return;
    }
  }

  /**
   * Cross-instance reload signals are ephemeral, so a replica that missed one
   * would keep serving its previous policy snapshot indefinitely. The shared
   * committed generation advances on every publication; comparing it here
   * converges a replica that lost the signal without operator action.
   */
  private async reconcileSharedGeneration(): Promise<void> {
    if (this.reconcilingSharedGeneration) return;
    if (!this.usesSharedRuntimeCache()) return;
    const store = this.redisRuntimeCacheStore;
    if (typeof store?.readCommittedGeneration !== 'function') return;
    this.reconcilingSharedGeneration = true;
    try {
      const generation = await store.readCommittedGeneration();
      if (!generation) return;
      if (this.appliedSharedGeneration === null) {
        this.appliedSharedGeneration = generation;
        return;
      }
      if (generation === this.appliedSharedGeneration) return;
      this.logger.warn(
        `Shared runtime cache generation advanced (${this.appliedSharedGeneration} -> ${generation}); replaying committed snapshots`,
      );
      await this.reloadAllLocal(false, true);
      this.appliedSharedGeneration = generation;
    } catch (error) {
      this.logger.error('Shared runtime cache generation reconcile failed:', error);
    } finally {
      this.reconcilingSharedGeneration = false;
    }
  }
}
