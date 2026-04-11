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

const COLOR = '\x1b[33m';
const RESET = '\x1b[0m';
const SYNC_CHANNEL = 'enfyra:cache-orchestrator-sync';

type ReloadStep = (payload: TCacheInvalidationPayload) => Promise<void>;

const RELOAD_CHAINS: Record<string, string[]> = {
  table_definition: ['metadata', 'repoRegistry', 'route', 'graphql'],
  column_definition: ['metadata', 'repoRegistry', 'route', 'graphql'],
  relation_definition: ['metadata', 'repoRegistry', 'route', 'graphql'],

  route_definition: ['route', 'graphql'],
  pre_hook_definition: ['route', 'graphql'],
  post_hook_definition: ['route', 'graphql'],
  route_handler_definition: ['route', 'graphql'],
  route_permission_definition: ['route', 'graphql'],
  role_definition: ['route', 'graphql'],
  method_definition: ['route', 'graphql'],

  guard_definition: ['guard'],
  guard_rule_definition: ['guard'],

  field_permission_definition: ['fieldPermission'],

  setting_definition: ['setting', 'graphql'],
  storage_config_definition: ['storage'],
  oauth_config_definition: ['oauth'],
  websocket_definition: ['websocket'],
  websocket_event_definition: ['websocket'],
  package_definition: ['package'],
  flow_definition: ['flow'],
  flow_step_definition: ['flow'],
  folder_definition: ['folder'],
  bootstrap_script_definition: ['bootstrap'],
};

@Injectable()
export class CacheOrchestratorService
  implements OnModuleInit, OnApplicationBootstrap
{
  private readonly logger = new Logger(`${COLOR}CacheOrchestrator${RESET}`);
  private stepMap: Record<string, ReloadStep>;
  private graphqlService: any;
  private bootstrapScriptService: any;
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
      bootstrap: () => this.reloadBootstrapScripts(),
    };
  }

  async onModuleInit() {
    this.subscribeToRedis();
  }

  async onApplicationBootstrap() {
    try {
      this.graphqlService = this.moduleRef.get('GraphqlService', {
        strict: false,
      });
    } catch {}
    try {
      this.bootstrapScriptService = this.moduleRef.get(
        'BootstrapScriptService',
        { strict: false },
      );
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
    if (
      this.pendingPayload.tableName !== payload.tableName ||
      payload.scope === 'full' ||
      this.pendingPayload.scope === 'full'
    ) {
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
    const chain = RELOAD_CHAINS[payload.tableName];
    if (!chain) {
      this.logger.debug(`No reload chain for table: ${payload.tableName}`);
      return;
    }

    if (publish) {
      await this.publishSignal(payload);
    }

    const start = Date.now();
    for (const step of chain) {
      const fn = this.stepMap[step];
      if (fn) {
        await fn(payload);
      }
    }

    const elapsed = Date.now() - start;
    this.logger.log(
      `${payload.scope === 'partial' ? 'Partial' : 'Full'} chain [${chain.join(' → ')}] for ${payload.tableName} in ${elapsed}ms`,
    );
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

  private async reloadBootstrapScripts(): Promise<void> {
    if (!this.bootstrapScriptService) return;
    await this.bootstrapScriptService.reloadBootstrapScripts();
  }

  async reloadAll(): Promise<void> {
    const start = Date.now();
    await this.metadataCache.reload();
    await this.reloadRepoRegistry();
    await this.routeCache.reload(false);
    await this.guardCache.reload(false);
    if (this.graphqlService) {
      await this.graphqlService.reloadSchema();
    }
    this.logger.log(`Full reload all in ${Date.now() - start}ms`);
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
          this.logger.debug(
            `Signal from ${signal.instanceId.slice(0, 8)}: ${signal.payload?.tableName}`,
          );
          await this.executeChain(signal.payload, false);
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
