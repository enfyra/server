import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { RedisPubSubService } from './redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
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
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { DynamicWebSocketGateway } from '../../../modules/websocket/gateway/dynamic-websocket.gateway';
import { GraphqlService } from '../../../modules/graphql/services/graphql.service';
import { BootstrapScriptService } from '../../../domain/bootstrap/services/bootstrap-script.service';
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

type ReloadStep = (payload: TCacheInvalidationPayload) => Promise<void>;

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
  private stepMap: Record<string, ReloadStep>;
  private messageHandler: ((channel: string, message: string) => void) | null =
    null;

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

    this.stepMap = {
      metadata: (p) => this.reloadMetadata(p),
      repoRegistry: () => this.reloadRepoRegistry(),
      route: (p) => this.reloadRoute(p),
      graphql: (p) => this.reloadGraphql(p),
      guard: (p) => this.reloadSimple(this.guardCacheService, p),
      flow: (p) => this.reloadSimple(this.flowCacheService, p),
      websocket: (p) => this.reloadSimple(this.websocketCacheService, p),
      package: (p) => this.reloadSimple(this.packageCacheService, p),
      setting: (p) => this.reloadSimple(this.settingCacheService, p),
      storage: (p) => this.reloadSimple(this.storageConfigCacheService, p),
      oauth: (p) => this.reloadSimple(this.oauthConfigCacheService, p),
      folder: (p) => this.reloadSimple(this.folderTreeCacheService, p),
      fieldPermission: (p) =>
        this.reloadSimple(this.fieldPermissionCacheService, p),
      'column-rule': (p) => this.reloadSimple(this.columnRuleCacheService, p),
      settingGraphql: () => this.reloadSettingGraphql(),
      bootstrap: () => this.reloadBootstrapScripts(),
    };

    deps.eventEmitter.on(
      CACHE_EVENTS.INVALIDATE,
      this.handleInvalidation.bind(this),
    );
  }

  async init() {
    this.subscribeToRedis();
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

    if (publish) {
      await this.publishSignal(payload);
    }

    const flow = this.resolveFlowName(chain);

    if (publish) {
      this.notifyClients('pending', flow, chain);
    }

    if (this.reloadLock) {
      try {
        await this.reloadLock;
      } catch {}
    }

    let elapsed = 0;
    this.reloadLock = (async () => {
      const start = Date.now();
      const stepTimings: string[] = [];

      if (chain.includes('metadata')) {
        const s = Date.now();
        await this.stepMap['metadata'](payload);
        stepTimings.push(`metadata:${Date.now() - s}ms`);
      }

      const middleSteps = chain.filter(
        (s) => s !== 'metadata' && s !== 'graphql',
      );
      if (middleSteps.length > 0) {
        const s = Date.now();
        await Promise.all(
          middleSteps.map(async (step) => {
            const fn = this.stepMap[step];
            if (fn) await fn(payload);
          }),
        );
        stepTimings.push(`[${middleSteps.join('+')}]:${Date.now() - s}ms`);
      }

      if (chain.includes('graphql')) {
        const s = Date.now();
        await this.stepMap['graphql'](payload);
        stepTimings.push(`graphql:${Date.now() - s}ms`);
      }

      elapsed = Date.now() - start;
      this.logger.log(
        `${payload.scope === 'partial' ? 'Partial' : 'Full'} chain [${stepTimings.join(' → ')}] for ${payload.table} in ${elapsed}ms`,
      );
    })();

    try {
      await this.reloadLock;
    } finally {
      this.reloadLock = null;
      if (publish) {
        if (elapsed < 500) {
          await new Promise((r) => setTimeout(r, 500 - elapsed));
        }
        this.notifyClients('done', flow, chain);
      }
    }
  }

  private resolveFlowName(chain: string[]): string {
    for (const step of FLOW_PRIORITY) {
      if (chain.includes(step)) return step;
    }
    return chain[0] ?? 'unknown';
  }

  private notifyClients(
    status: 'pending' | 'done',
    flow: string,
    steps?: string[],
  ): void {
    try {
      this.dynamicWebSocketGateway?.emitToNamespace?.(
        ENFYRA_ADMIN_WEBSOCKET_NAMESPACE,
        '$system:reload',
        { flow, status, steps },
      );
    } catch {}
  }

  private async reloadMetadata(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
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
    this.repoRegistryService.rebuildFromMetadata(this.metadataCacheService);
  }

  private async reloadRoute(payload: TCacheInvalidationPayload): Promise<void> {
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
  ): Promise<void> {
    if (!this.graphqlService) return;
    await this.graphqlService.reloadSchema(payload);
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
    },
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
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
    this.graphqlService?.onSettingChanged?.();
  }

  private async reloadBootstrapScripts(): Promise<void> {
    if (!this.bootstrapScriptService) return;
    await this.bootstrapScriptService.reloadBootstrapScripts();
  }

  async reloadMetadataAndDeps(): Promise<void> {
    const start = Date.now();
    const steps = ['metadata', 'repoRegistry', 'route', 'graphql'];
    this.notifyClients('pending', 'metadata', steps);
    await this.metadataCacheService.reload();
    await this.reloadRepoRegistry();
    await this.routeCacheService.reload(false);
    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
    this.notifyClients('done', 'metadata', steps);
    this.logger.log(`Admin reload metadata+deps: ${Date.now() - start}ms`);
    await this.publishSignal({
      table: 'table_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadRoutesOnly(): Promise<void> {
    const start = Date.now();
    const steps = ['route'];
    this.notifyClients('pending', 'route', steps);
    await this.routeCacheService.reload(false);
    this.notifyClients('done', 'route', steps);
    this.logger.log(`Admin reload routes: ${Date.now() - start}ms`);
    await this.publishSignal({
      table: 'route_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadGraphqlOnly(): Promise<void> {
    const start = Date.now();
    const steps = ['graphql'];
    this.notifyClients('pending', 'graphql', steps);
    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
    this.notifyClients('done', 'graphql', steps);
    this.logger.log(`Admin reload graphql: ${Date.now() - start}ms`);
  }

  async reloadGuardsOnly(): Promise<void> {
    const start = Date.now();
    const steps = ['guard'];
    this.notifyClients('pending', 'guard', steps);
    await this.guardCacheService.reload(false);
    this.notifyClients('done', 'guard', steps);
    this.logger.log(`Admin reload guards: ${Date.now() - start}ms`);
    await this.publishSignal({
      table: 'guard_definition',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
  }

  async reloadAll(): Promise<void> {
    await this.publishSignal({
      table: '__admin_reload_all',
      action: 'reload',
      scope: 'full',
      timestamp: Date.now(),
    });
    await this.reloadAllLocal(true);
  }

  private async reloadAllLocal(notify = false): Promise<void> {
    const start = Date.now();
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
    if (notify) this.notifyClients('pending', 'all', steps);
    await this.metadataCacheService.reload();
    await Promise.all([
      this.reloadRepoRegistry(),
      this.routeCacheService.reload(false),
      this.guardCacheService.reload(false),
      this.flowCacheService.reload(false),
      this.websocketCacheService.reload(false),
      this.packageCacheService.reload(false),
      this.settingCacheService.reload(false),
      this.storageConfigCacheService.reload(false),
      this.oauthConfigCacheService.reload(false),
      this.folderTreeCacheService.reload(false),
      this.fieldPermissionCacheService.reload(false),
      this.columnRuleCacheService.reload(false),
    ]);
    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
    if (notify) {
      const elapsed = Date.now() - start;
      if (elapsed < 200) {
        await new Promise((r) => setTimeout(r, 200 - elapsed));
      }
      this.notifyClients('done', 'all', steps);
    }
    this.logger.log(`Admin reload ALL: ${Date.now() - start}ms`);
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
            const first = this.processedVersions.values().next().value;
            this.processedVersions.delete(first);
          }
          this.logger.log(
            `Redis signal from ${signal.instanceId.slice(0, 8)}: ${signal.payload?.table} (${signal.payload?.scope || 'full'})`,
          );
          if (signal.payload?.table === '__admin_reload_all') {
            await this.reloadAllLocal();
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
        const first = this.processedVersions.values().next().value;
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
}
