import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { RedisPubSubService } from './redis-pubsub.service';
import {
  InstanceService,
  RuntimeMetricsCollectorService,
} from '../../../shared/services';
import { MetadataCacheService } from './metadata-cache.service';
import { RouteCacheService } from './route-cache.service';
import { GuardCacheService } from './guard-cache.service';
import { FlowCacheService } from './flow-cache.service';
import { WebsocketCacheService } from './websocket-cache.service';
import { PackageCacheService } from './package-cache.service';
import { SettingCacheService } from './setting-cache.service';
import { StorageConfigCacheService } from './storage-config-cache.service';
import { OAuthConfigCacheService } from './oauth-config-cache.service';
import { FolderTreeCacheService } from './folder-tree-cache.service';
import { FieldPermissionCacheService } from './field-permission-cache.service';
import { ColumnRuleCacheService } from './column-rule-cache.service';
import { RepoRegistryService } from './repo-registry.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { DynamicWebSocketGateway } from '../../../modules/websocket';
import { GraphqlService } from '../../../modules/graphql';
import { BootstrapScriptService } from '../../../domain/bootstrap';
import { LifecycleAware } from '../../../shared/interfaces/lifecycle-aware.interface';

const COLOR = '\x1b[33m';
const RESET = '\x1b[0m';
const SYNC_CHANNEL = 'enfyra:cache-orchestrator-sync';

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
  table_definition: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
  ],
  column_definition: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
  ],
  relation_definition: [
    'metadata',
    'repoRegistry',
    'route',
    'graphql',
    'fieldPermission',
    'column-rule',
  ],

  route_definition: ['route', 'graphql', 'guard'],
  pre_hook_definition: ['route'],
  post_hook_definition: ['route'],
  route_handler_definition: ['route'],
  route_permission_definition: ['route'],
  role_definition: ['route'],
  method_definition: ['route', 'graphql'],

  guard_definition: ['guard'],
  guard_rule_definition: ['guard'],

  field_permission_definition: ['fieldPermission', 'graphql'],

  column_rule_definition: ['column-rule'],

  setting_definition: ['setting', 'settingGraphql'],
  storage_config_definition: ['storage'],
  oauth_config_definition: ['oauth'],
  websocket_definition: ['websocket'],
  websocket_event_definition: ['websocket'],
  package_definition: ['package'],
  flow_definition: ['flow'],
  flow_step_definition: ['flow'],
  folder_definition: ['folder'],
  bootstrap_script_definition: ['bootstrap'],
  gql_definition: ['graphql'],
};

export class CacheOrchestratorService implements LifecycleAware {
  private readonly logger = new Logger(`${COLOR}CacheOrchestrator${RESET}`);
  private readonly redisPubSubService: RedisPubSubService;
  private readonly instanceService: InstanceService;
  private readonly eventEmitter: EventEmitter2;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly routeCacheService: RouteCacheService;
  private readonly guardCacheService: GuardCacheService;
  private readonly flowCacheService: FlowCacheService;
  private readonly websocketCacheService: WebsocketCacheService;
  private readonly packageCacheService: PackageCacheService;
  private readonly settingCacheService: SettingCacheService;
  private readonly storageConfigCacheService: StorageConfigCacheService;
  private readonly oauthConfigCacheService: OAuthConfigCacheService;
  private readonly folderTreeCacheService: FolderTreeCacheService;
  private readonly fieldPermissionCacheService: FieldPermissionCacheService;
  private readonly columnRuleCacheService: ColumnRuleCacheService;
  private readonly repoRegistryService: RepoRegistryService;
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
  private processedVersions: Set<string> = new Set();

  constructor(deps: {
    redisPubSubService: RedisPubSubService;
    instanceService: InstanceService;
    eventEmitter: EventEmitter2;
    metadataCacheService: MetadataCacheService;
    routeCacheService: RouteCacheService;
    guardCacheService: GuardCacheService;
    flowCacheService: FlowCacheService;
    websocketCacheService: WebsocketCacheService;
    packageCacheService: PackageCacheService;
    settingCacheService: SettingCacheService;
    storageConfigCacheService: StorageConfigCacheService;
    oauthConfigCacheService: OAuthConfigCacheService;
    folderTreeCacheService: FolderTreeCacheService;
    fieldPermissionCacheService: FieldPermissionCacheService;
    columnRuleCacheService: ColumnRuleCacheService;
    repoRegistryService: RepoRegistryService;
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
    this.guardCacheService = deps.guardCacheService;
    this.flowCacheService = deps.flowCacheService;
    this.websocketCacheService = deps.websocketCacheService;
    this.packageCacheService = deps.packageCacheService;
    this.settingCacheService = deps.settingCacheService;
    this.storageConfigCacheService = deps.storageConfigCacheService;
    this.oauthConfigCacheService = deps.oauthConfigCacheService;
    this.folderTreeCacheService = deps.folderTreeCacheService;
    this.fieldPermissionCacheService = deps.fieldPermissionCacheService;
    this.columnRuleCacheService = deps.columnRuleCacheService;
    this.repoRegistryService = deps.repoRegistryService;
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
        this.reloadSimple(this.guardCacheService, p, options?.sharedReplay),
      flow: (p, options) =>
        this.reloadSimple(this.flowCacheService, p, options?.sharedReplay),
      websocket: (p, options) =>
        this.reloadSimple(this.websocketCacheService, p, options?.sharedReplay),
      package: (p, options) =>
        this.reloadSimple(this.packageCacheService, p, options?.sharedReplay),
      setting: (p, options) =>
        this.reloadSimple(this.settingCacheService, p, options?.sharedReplay),
      storage: (p, options) =>
        this.reloadSimple(
          this.storageConfigCacheService,
          p,
          options?.sharedReplay,
        ),
      oauth: (p, options) =>
        this.reloadSimple(
          this.oauthConfigCacheService,
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
          this.fieldPermissionCacheService,
          p,
          options?.sharedReplay,
        ),
      'column-rule': (p, options) =>
        this.reloadSimple(this.columnRuleCacheService, p, options?.sharedReplay),
      settingGraphql: () => this.reloadSettingGraphql(),
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
      return;
    }
    if (payload.scope === 'full' || this.pendingPayload.scope === 'full') {
      this.pendingPayload.scope = 'full';
      this.pendingPayload.ids = undefined;
      this.pendingPayload.affectedTables = undefined;
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
    this.pendingPayload.ids = [...mergedIds];
    this.pendingPayload.affectedTables = mergedTables.size
      ? [...mergedTables]
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
    this.notifyClients('pending', flow, chain, reloadId);

    if (this.reloadLock) {
      try {
        await this.reloadLock;
      } catch {}
    }

    let elapsed = 0;
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

      const runStep = async (name: string, fn: () => Promise<void>) => {
        const s = Date.now();
        try {
          await fn();
          const durationMs = Date.now() - s;
          steps.push({ name, durationMs, status: 'success' });
          return durationMs;
        } catch (error: any) {
          const durationMs = Date.now() - s;
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
          (s) => s !== 'metadata' && s !== 'graphql',
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
    };
    this.reloadLock = this.runtimeMetricsCollectorService
      ? this.runtimeMetricsCollectorService.runWithQueryContext(
          'cache',
          runReload,
        )
      : runReload();

    try {
      await this.reloadLock;
      if (publish && sharedRuntimeCache) {
        await this.publishSignal(payload);
      }
    } finally {
      this.reloadLock = null;
      if (elapsed < 500) {
        await new Promise((r) => setTimeout(r, 500 - elapsed));
      }
      this.notifyClients('done', flow, chain, reloadId);
    }
  }

  private resolveFlowName(chain: string[]): string {
    for (const step of FLOW_PRIORITY) {
      if (chain.includes(step)) return step;
    }
    return chain[0] ?? 'unknown';
  }

  private createReloadEventId(flow: string): string {
    return `${this.instanceService.getInstanceId()}:${flow}:${Date.now()}`;
  }

  private notifyClients(
    status: 'pending' | 'done',
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
    await this.repoRegistryService.rebuildFromMetadata(this.metadataCacheService);
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
    if (!this.graphqlService) return;
    await this.graphqlService.reloadSchema(payload, {
      definitionFromSharedCache: sharedReplay,
    });
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
      this.notifyClients('pending', input.flow, input.steps, reloadId);
      try {
        await input.run(tracker.runStep);
      } catch (error) {
        reloadError = error;
        throw error;
      } finally {
        tracker.finish(reloadError);
        this.notifyClients('done', input.flow, input.steps, reloadId);
      }
      this.logger.log(`${input.logLabel}: ${tracker.elapsed()}ms`);
    };

    if (this.runtimeMetricsCollectorService) {
      await this.runtimeMetricsCollectorService.runWithQueryContext(
        'cache',
        runReload,
      );
      return;
    }
    await runReload();
  }

  async reloadMetadataAndDeps(): Promise<void> {
    const steps = ['metadata', 'repoRegistry', 'route', 'graphql'];
    await this.runTrackedAdminReload({
      flow: 'metadata',
      table: 'table_definition',
      steps,
      logLabel: 'Admin reload metadata+deps',
      run: async (runStep) => {
        await runStep('metadata', () => this.metadataCacheService.reload());
        await runStep('repoRegistry', () => this.reloadRepoRegistry());
        await runStep('route', () => this.routeCacheService.reload(false));
        if (this.graphqlService) {
          await runStep('graphql', () => this.graphqlService.reloadSchema());
        }
      },
    });
    await this.publishSignal({
      table: 'table_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadRoutesOnly(): Promise<void> {
    const steps = ['route'];
    await this.runTrackedAdminReload({
      flow: 'route',
      table: 'route_definition',
      steps,
      logLabel: 'Admin reload routes',
      run: async (runStep) => {
        await runStep('route', () => this.routeCacheService.reload(false));
      },
    });
    await this.publishSignal({
      table: 'route_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadGraphqlOnly(): Promise<void> {
    const steps = ['graphql'];
    await this.runTrackedAdminReload({
      flow: 'graphql',
      table: 'gql_definition',
      steps,
      logLabel: 'Admin reload graphql',
      run: async (runStep) => {
        if (this.graphqlService) {
          await runStep('graphql', () => this.graphqlService.reloadSchema());
        }
      },
    });
    await this.publishSignal({
      table: 'gql_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadGuardsOnly(): Promise<void> {
    const steps = ['guard'];
    await this.runTrackedAdminReload({
      flow: 'guard',
      table: 'guard_definition',
      steps,
      logLabel: 'Admin reload guards',
      run: async (runStep) => {
        await runStep('guard', () => this.guardCacheService.reload(false));
      },
    });
    await this.publishSignal({
      table: 'guard_definition',
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
        if (notify) this.notifyClients('pending', 'all', steps, reloadId);
        await runStep('metadata', () =>
          sharedReplay
            ? this.metadataCacheService.syncFromSharedCache()
            : this.metadataCacheService.reload(),
        );
        await Promise.all([
          runStep('repoRegistry', () => this.reloadRepoRegistry()),
          runStep('route', () =>
            sharedReplay
              ? this.routeCacheService.syncFromSharedCache()
              : this.routeCacheService.reload(false),
          ),
          runStep('guard', () =>
            sharedReplay
              ? this.guardCacheService.syncFromSharedCache()
              : this.guardCacheService.reload(false),
          ),
          runStep('flow', () =>
            sharedReplay
              ? this.flowCacheService.syncFromSharedCache()
              : this.flowCacheService.reload(false),
          ),
          runStep('websocket', () =>
            sharedReplay
              ? this.websocketCacheService.syncFromSharedCache()
              : this.websocketCacheService.reload(false),
          ),
          runStep('package', () =>
            sharedReplay
              ? this.packageCacheService.syncFromSharedCache()
              : this.packageCacheService.reload(false),
          ),
          runStep('setting', () =>
            sharedReplay
              ? this.settingCacheService.syncFromSharedCache()
              : this.settingCacheService.reload(false),
          ),
          runStep('storage', () =>
            sharedReplay
              ? this.storageConfigCacheService.syncFromSharedCache()
              : this.storageConfigCacheService.reload(false),
          ),
          runStep('oauth', () =>
            sharedReplay
              ? this.oauthConfigCacheService.syncFromSharedCache()
              : this.oauthConfigCacheService.reload(false),
          ),
          runStep('folder', () =>
            sharedReplay
              ? this.folderTreeCacheService.syncFromSharedCache()
              : this.folderTreeCacheService.reload(false),
          ),
          runStep('fieldPermission', () =>
            sharedReplay
              ? this.fieldPermissionCacheService.syncFromSharedCache()
              : this.fieldPermissionCacheService.reload(false),
          ),
          runStep('columnRule', () =>
            sharedReplay
              ? this.columnRuleCacheService.syncFromSharedCache()
              : this.columnRuleCacheService.reload(false),
          ),
        ]);
        if (this.graphqlService) {
          await runStep('graphql', () =>
            this.graphqlService.reloadSchema(undefined, {
              definitionFromSharedCache: sharedReplay,
            }),
          );
        }
        if (notify) {
          const elapsed = Date.now() - start;
          if (elapsed < 200) {
            await new Promise((r) => setTimeout(r, 200 - elapsed));
          }
          this.notifyClients('done', 'all', steps, reloadId);
        }
      } catch (error) {
        reloadError = error;
        throw error;
      } finally {
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
    if (this.runtimeMetricsCollectorService) {
      await this.runtimeMetricsCollectorService.runWithQueryContext(
        'cache',
        runReload,
      );
      return;
    }
    await runReload();
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
