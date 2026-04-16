import {
  Injectable,
  Logger,
  OnApplicationBootstrap,
  OnModuleInit,
} from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
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
import { RepoRegistryService } from './repo-registry.service';
import { TCacheInvalidationPayload } from '../../../shared/types/cache.types';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import { ENFYRA_ADMIN_WEBSOCKET_NAMESPACE } from '../../../shared/utils/constant';
import { DynamicWebSocketGateway } from '../../../modules/websocket/gateway/dynamic-websocket.gateway';
import { GraphqlService } from '../../../modules/graphql/services/graphql.service';
import { BootstrapScriptService } from '../../../core/bootstrap/services/bootstrap-script.service';

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

const RELOAD_CHAINS: Record<string, string[]> = {
  table_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
  column_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],
  relation_definition: ['metadata', 'repoRegistry', 'route', 'graphql', 'fieldPermission'],

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

@Injectable()
export class CacheOrchestratorService
  implements OnModuleInit, OnApplicationBootstrap
{
  private readonly logger = new Logger(`${COLOR}CacheOrchestrator${RESET}`);
  private stepMap: Record<string, ReloadStep>;
  private graphqlService: any;
  private bootstrapScriptService: any;
  private websocketGateway: any;
  private messageHandler: ((channel: string, message: string) => void) | null =
    null;

  private debounceTimer: ReturnType<typeof setTimeout> | null = null;
  private debounceResolvers: Array<() => void> = [];
  private pendingPayload: TCacheInvalidationPayload | null = null;

  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly redisPubSubService: RedisPubSubService,
    private readonly instanceService: InstanceService,
    private readonly eventEmitter: EventEmitter2,
    private readonly metadataCache: MetadataCacheService,
    private readonly routeCache: RouteCacheService,
    private readonly guardCache: GuardCacheService,
    private readonly flowCache: FlowCacheService,
    private readonly websocketCache: WebsocketCacheService,
    private readonly packageCache: PackageCacheService,
    private readonly settingCache: SettingCacheService,
    private readonly storageCache: StorageConfigCacheService,
    private readonly oauthCache: OAuthConfigCacheService,
    private readonly folderCache: FolderTreeCacheService,
    private readonly fieldPermissionCache: FieldPermissionCacheService,
    private readonly repoRegistry: RepoRegistryService,
  ) {
    this.stepMap = {
      metadata: (p) => this.reloadMetadata(p),
      repoRegistry: () => this.reloadRepoRegistry(),
      route: (p) => this.reloadRoute(p),
      graphql: (p) => this.reloadGraphql(p),
      guard: (p) => this.reloadSimple(this.guardCache, p),
      flow: (p) => this.reloadSimple(this.flowCache, p),
      websocket: (p) => this.reloadSimple(this.websocketCache, p),
      package: (p) => this.reloadSimple(this.packageCache, p),
      setting: (p) => this.reloadSimple(this.settingCache, p),
      storage: (p) => this.reloadSimple(this.storageCache, p),
      oauth: (p) => this.reloadSimple(this.oauthCache, p),
      folder: (p) => this.reloadSimple(this.folderCache, p),
      fieldPermission: (p) => this.reloadSimple(this.fieldPermissionCache, p),
      settingGraphql: () => this.reloadSettingGraphql(),
      bootstrap: () => this.reloadBootstrapScripts(),
    };
  }

  async onModuleInit() {
    this.subscribeToRedis();
  }

  async onApplicationBootstrap() {
    try {
      this.graphqlService = this.moduleRef.get(GraphqlService, {
        strict: false,
      });
    } catch {}
    try {
      this.bootstrapScriptService = this.moduleRef.get(BootstrapScriptService, {
        strict: false,
      });
    } catch {}
    try {
      this.websocketGateway = this.moduleRef.get(DynamicWebSocketGateway, {
        strict: false,
      });
    } catch {}

    await this.bootstrap();
  }

  private async bootstrap(): Promise<void> {
    const start = Date.now();
    await this.metadataCache.reload();
    this.eventEmitter.emit(CACHE_EVENTS.METADATA_LOADED);

    await Promise.all([
      this.routeCache.reload(false),
      this.guardCache.reload(false),
      this.flowCache.reload(false),
      this.websocketCache.reload(false),
      this.packageCache.reload(false),
      this.settingCache.reload(false),
      this.storageCache.reload(false),
      this.oauthCache.reload(false),
      this.folderCache.reload(false),
      this.reloadRepoRegistry(),
      this.bootstrapScriptService?.onMetadataLoaded?.(),
    ]);

    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
    this.eventEmitter.emit(CACHE_EVENTS.GRAPHQL_LOADED);

    this.logger.log(`Bootstrap completed in ${Date.now() - start}ms`);
    this.eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  handleInvalidation(payload: TCacheInvalidationPayload): Promise<void> {
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

    const start = Date.now();
    const stepTimings: string[] = [];

    // Phase 1: metadata must run first (others depend on it)
    if (chain.includes('metadata')) {
      const s = Date.now();
      await this.stepMap['metadata'](payload);
      stepTimings.push(`metadata:${Date.now() - s}ms`);
    }

    // Phase 2: middle steps run in parallel (all depend on metadata, not each other)
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

    // Phase 3: graphql must run last (depends on metadata + route)
    if (chain.includes('graphql')) {
      const s = Date.now();
      await this.stepMap['graphql'](payload);
      stepTimings.push(`graphql:${Date.now() - s}ms`);
    }

    const elapsed = Date.now() - start;
    this.logger.log(
      `${payload.scope === 'partial' ? 'Partial' : 'Full'} chain [${stepTimings.join(' → ')}] for ${payload.table} in ${elapsed}ms`,
    );

    if (publish) {
      if (elapsed < 500) {
        await new Promise((r) => setTimeout(r, 500 - elapsed));
      }
      this.notifyClients('done', flow, chain);
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
      this.websocketGateway?.emitToNamespace?.(
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
      this.metadataCache.isLoaded()
    ) {
      await this.metadataCache.partialReload(payload);
    } else {
      await this.metadataCache.reload();
    }
  }

  private async reloadRepoRegistry(): Promise<void> {
    this.repoRegistry.rebuildFromMetadata(this.metadataCache);
  }

  private async reloadRoute(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (
      payload.scope === 'partial' &&
      this.routeCache.isLoaded() &&
      this.routeCache.supportsPartialReload()
    ) {
      await this.routeCache.partialReload(payload, false);
    } else {
      await this.routeCache.reload(false);
    }
  }

  private async reloadGraphql(
    payload: TCacheInvalidationPayload,
  ): Promise<void> {
    if (!this.graphqlService) return;
    await this.graphqlService.reloadSchema(payload);
  }

  private async reloadSimple(
    cache: { reload: (publish?: boolean) => Promise<void> },
    _payload: TCacheInvalidationPayload,
  ): Promise<void> {
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
    await this.metadataCache.reload();
    await this.reloadRepoRegistry();
    await this.routeCache.reload(false);
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
    await this.routeCache.reload(false);
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
    await this.guardCache.reload(false);
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
    await this.metadataCache.reload();
    await Promise.all([
      this.reloadRepoRegistry(),
      this.routeCache.reload(false),
      this.guardCache.reload(false),
      this.flowCache.reload(false),
      this.websocketCache.reload(false),
      this.packageCache.reload(false),
      this.settingCache.reload(false),
      this.storageCache.reload(false),
      this.oauthCache.reload(false),
      this.folderCache.reload(false),
      this.fieldPermissionCache.reload(false),
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
      if (
        this.redisPubSubService.isChannelForBase(channel, SYNC_CHANNEL)
      ) {
        try {
          const signal = JSON.parse(message);
          if (signal.instanceId === this.instanceService.getInstanceId()) {
            return;
          }
          this.logger.log(
            `Redis signal from ${signal.instanceId.slice(0, 8)}: ${signal.payload?.tableName} (${signal.payload?.scope || 'full'})`,
          );
          if (signal.payload?.tableName === '__admin_reload_all') {
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
      await this.redisPubSubService.publish(
        SYNC_CHANNEL,
        JSON.stringify({
          instanceId: this.instanceService.getInstanceId(),
          type: 'RELOAD_SIGNAL',
          timestamp: Date.now(),
          payload,
        }),
      );
    } catch (error) {
      this.logger.error('Failed to publish signal:', error);
    }
  }
}
