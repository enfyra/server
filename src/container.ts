import {
  createContainer,
  asClass,
  asValue,
  asFunction,
  InjectionMode,
  AwilixContainer,
} from 'awilix';
import { EventEmitter2 } from 'eventemitter2';
import Redis from 'ioredis';
import { Queue } from 'bullmq';
import { env } from './env';
import { SYSTEM_QUEUES } from './shared/utils/constant';

type QueueWithConnection = Queue & { __enfyraConnection?: Redis };

function createRuntimeQueue(name: string): QueueWithConnection {
  const connection = new Redis(env.REDIS_URI);
  const queue = new Queue(name, {
    prefix: `${env.NODE_NAME}:`,
    connection,
  }) as QueueWithConnection;
  queue.__enfyraConnection = connection;
  return queue;
}

async function closeRuntimeQueue(queue: QueueWithConnection): Promise<void> {
  await queue.close();
  queue.__enfyraConnection?.disconnect();
}

import { BcryptService } from './domain/auth';
import { AuthService } from './domain/auth';
import { OAuthService } from './domain/auth';
import { SessionCleanupService } from './domain/auth';
import { UserRevocationService } from './domain/auth';

import { BootstrapScriptDefinitionProcessor } from './domain/bootstrap';
import { ExtensionDefinitionProcessor } from './domain/bootstrap';
import { FlowDefinitionProcessor } from './domain/bootstrap';
import { FlowExecutionDefinitionProcessor } from './domain/bootstrap';
import { FlowStepDefinitionProcessor } from './domain/bootstrap';
import { FolderDefinitionProcessor } from './domain/bootstrap';
import { GenericTableProcessor } from './domain/bootstrap';
import { GraphQLDefinitionProcessor } from './domain/bootstrap';
import { HookDefinitionProcessor } from './domain/bootstrap';
import { MenuDefinitionProcessor } from './domain/bootstrap';
import { MethodDefinitionProcessor } from './domain/bootstrap';
import { PostHookDefinitionProcessor } from './domain/bootstrap';
import { PreHookDefinitionProcessor } from './domain/bootstrap';
import { FieldPermissionDefinitionProcessor } from './domain/bootstrap';
import { RouteDefinitionProcessor } from './domain/bootstrap';
import { RouteHandlerDefinitionProcessor } from './domain/bootstrap';
import { RoutePermissionDefinitionProcessor } from './domain/bootstrap';
import { SettingDefinitionProcessor } from './domain/bootstrap';
import { UserDefinitionProcessor } from './domain/bootstrap';
import { WebsocketDefinitionProcessor } from './domain/bootstrap';
import { WebsocketEventDefinitionProcessor } from './domain/bootstrap';

import { BootstrapScriptService } from './domain/bootstrap';
import { DataMigrationService } from './engine/bootstrap';
import { DataProvisionService } from './engine/bootstrap';
import { FirstRunInitializer } from './engine/bootstrap';
import { MetadataMigrationService } from './engine/bootstrap';
import { MetadataProvisionMongoService } from './engine/bootstrap';
import { MetadataProvisionSqlService } from './engine/bootstrap';
import { MetadataProvisionService } from './engine/bootstrap';
import { MetadataRepairService } from './engine/bootstrap';
import { ProvisionService } from './engine/bootstrap';

import { LoggingService } from './domain/exceptions';

import { PolicyService } from './domain/policy';
import { SchemaMigrationValidatorService } from './domain/policy';
import { SystemSafetyAuditorService } from './domain/policy';

import { CacheOrchestratorService } from './engine/cache';
import { CacheService } from './engine/cache';
import { FieldPermissionCacheService } from './engine/cache';
import { ColumnRuleCacheService } from './engine/cache';
import { FlowCacheService } from './engine/cache';
import { FolderTreeCacheService } from './engine/cache';
import { GqlDefinitionCacheService } from './engine/cache';
import { GuardCacheService } from './engine/cache';
import { GuardEvaluatorService } from './engine/cache';
import { MetadataCacheService } from './engine/cache';
import { OAuthConfigCacheService } from './engine/cache';
import { PackageCacheService } from './engine/cache';
import { PackageCdnLoaderService } from './engine/cache';
import { RateLimitService } from './engine/cache';
import { RedisPubSubService } from './engine/cache';
import { RepoRegistryService } from './engine/cache';
import { DynamicRepositoryFactory } from './modules/dynamic-api';
import { RouteCacheService } from './engine/cache';
import { SettingCacheService } from './engine/cache';
import { StorageConfigCacheService } from './engine/cache';
import { WebsocketCacheService } from './engine/cache';

import { ExecutorEngineService } from './kernel/execution';
import { IsolatedExecutorService } from './kernel/execution';

import { KnexService } from './engine/knex';
import { KnexHookManagerService } from './engine/knex';
import { MigrationJournalService } from './engine/knex';
import { ReplicationManager } from './engine/knex';
import { SchemaMigrationLockService } from './engine/knex';
import { SqlPoolClusterCoordinatorService } from './engine/knex';
import { SqlSchemaDiffService } from './engine/knex';
import { SqlSchemaMigrationService } from './engine/knex';
import { DatabaseSchemaService } from './engine/knex';

import { MongoMigrationJournalService } from './engine/mongo';
import { MongoOperationLogService } from './engine/mongo';
import { MongoRelationManagerService } from './engine/mongo';
import { MongoSagaCoordinator } from './engine/mongo';
import { MongoSagaLockService } from './engine/mongo';
import { MongoSchemaDiffService } from './engine/mongo';
import { MongoSchemaMigrationLockService } from './engine/mongo';
import { MongoSchemaMigrationService } from './engine/mongo';
import { MongoService } from './engine/mongo';

import { QueryBuilderService } from './kernel/query';

import { MongoQueryEngine } from './kernel/query';
import { QueryEngine } from './kernel/query';
import { SqlQueryEngine } from './kernel/query';

import { SqlFunctionService } from './engine/sql';

import { LogReaderService } from './modules/admin';
import { RuntimeMonitorService } from './modules/admin';
import { RuntimeDbMetricsService } from './modules/admin';
import { RuntimeProcessMetricsService } from './modules/admin';
import { RuntimeQueueMetricsService } from './modules/admin';

import { DynamicService } from './modules/dynamic-api';
import { DynamicApiTableValidationService } from './modules/dynamic-api';

import { FileManagementService } from './modules/file-management';
import { FileAssetsService } from './modules/file-management';
import { GCSStorageService } from './modules/file-management';
import { LocalStorageService } from './modules/file-management';
import { R2StorageService } from './modules/file-management';
import { S3StorageService } from './modules/file-management';
import { StorageFactoryService } from './modules/file-management';

import { FlowExecutionQueueService } from './modules/flow';
import { FlowSchedulerService } from './modules/flow';
import { FlowService } from './modules/flow';

import { GraphqlService } from './modules/graphql';
import { DynamicResolver } from './modules/graphql';

import { MeService } from './modules/me';

import { MongoMetadataSnapshotService } from './modules/table-management';
import { MongoTableHandlerService } from './modules/table-management';
import { SqlTableHandlerService } from './modules/table-management';
import { SqlTableMetadataBuilderService } from './modules/table-management';
import { SqlTableMetadataWriterService } from './modules/table-management';
import { TableHandlerService } from './modules/table-management';
import { TableManagementValidationService } from './modules/table-management';

import { WebsocketGatewayFactory } from './modules/websocket';
import { DynamicWebSocketGateway } from './modules/websocket';
import { ConnectionQueueService } from './modules/websocket';
import { EventQueueService } from './modules/websocket';
import { BuiltInSocketRegistry } from './modules/websocket';
import { WebsocketEmitService } from './modules/websocket';
import { WebsocketContextFactory } from './modules/websocket';

import { CommonService } from './shared/common';
import { DatabaseConfigService } from './shared/services';
import { UploadFileHelper } from './shared/helpers';
import { EnvService } from './shared/services';
import { InstanceService } from './shared/services';
import { DynamicContextFactory } from './shared/services';
import { RuntimeMetricsCollectorService } from './shared/services';
import { ClusterTelemetryService } from './shared/services';

export interface Cradle {
  envService: EnvService;
  eventEmitter: EventEmitter2;
  redis: Redis;

  commonService: CommonService;
  databaseConfigService: DatabaseConfigService;
  instanceService: InstanceService;
  dynamicContextFactory: DynamicContextFactory;
  runtimeMetricsCollectorService: RuntimeMetricsCollectorService;
  clusterTelemetryService: ClusterTelemetryService;
  configService: any;
  lazyRef: Cradle;
  bcryptService: BcryptService;
  authService: AuthService;
  oauthService: OAuthService;
  sessionCleanupService: SessionCleanupService;
  userRevocationService: UserRevocationService;
  loggingService: LoggingService;
  policyService: PolicyService;
  schemaMigrationValidatorService: SchemaMigrationValidatorService;
  systemSafetyAuditorService: SystemSafetyAuditorService;

  mongoService: MongoService;
  mongoSchemaMigrationService: MongoSchemaMigrationService;
  mongoSchemaMigrationLockService: MongoSchemaMigrationLockService;
  mongoSagaLockService: MongoSagaLockService;
  mongoSagaCoordinator: MongoSagaCoordinator;
  mongoOperationLogService: MongoOperationLogService;
  mongoMigrationJournalService: MongoMigrationJournalService;
  mongoSchemaDiffService: MongoSchemaDiffService;
  mongoRelationManagerService: MongoRelationManagerService;

  knexService: KnexService;
  knexHookManagerService: KnexHookManagerService;
  replicationManager: ReplicationManager;
  sqlSchemaMigrationService: SqlSchemaMigrationService;
  sqlSchemaDiffService: SqlSchemaDiffService;
  migrationJournalService: MigrationJournalService;
  databaseSchemaService: DatabaseSchemaService;
  schemaMigrationLockService: SchemaMigrationLockService;
  sqlPoolClusterCoordinatorService: SqlPoolClusterCoordinatorService;
  sqlFunctionService: SqlFunctionService;

  queryBuilderService: QueryBuilderService;
  queryEngine: QueryEngine;
  sqlQueryEngine: SqlQueryEngine;
  mongoQueryEngine: MongoQueryEngine;

  isolatedExecutorService: IsolatedExecutorService;
  executorEngineService: ExecutorEngineService;

  cacheService: CacheService;
  redisPubSubService: RedisPubSubService;
  metadataCacheService: MetadataCacheService;
  routeCacheService: RouteCacheService;
  packageCacheService: PackageCacheService;
  storageConfigCacheService: StorageConfigCacheService;
  websocketCacheService: WebsocketCacheService;
  oauthConfigCacheService: OAuthConfigCacheService;
  rateLimitService: RateLimitService;
  folderTreeCacheService: FolderTreeCacheService;
  flowCacheService: FlowCacheService;
  packageCdnLoaderService: PackageCdnLoaderService;
  guardCacheService: GuardCacheService;
  guardEvaluatorService: GuardEvaluatorService;
  settingCacheService: SettingCacheService;
  fieldPermissionCacheService: FieldPermissionCacheService;
  columnRuleCacheService: ColumnRuleCacheService;
  gqlDefinitionCacheService: GqlDefinitionCacheService;
  repoRegistryService: RepoRegistryService;
  dynamicRepositoryFactory: DynamicRepositoryFactory;
  cacheOrchestratorService: CacheOrchestratorService;

  tableHandlerService: TableHandlerService;
  sqlTableHandlerService: SqlTableHandlerService;
  mongoTableHandlerService: MongoTableHandlerService;
  tableValidationService: DynamicApiTableValidationService;
  tableManagementValidationService: TableManagementValidationService;
  mongoMetadataSnapshotService: MongoMetadataSnapshotService;
  sqlTableMetadataBuilderService: SqlTableMetadataBuilderService;
  sqlTableMetadataWriterService: SqlTableMetadataWriterService;

  dynamicService: DynamicService;

  fileManagementService: FileManagementService;
  fileAssetsService: FileAssetsService;
  localStorageService: LocalStorageService;
  gcsStorageService: GCSStorageService;
  r2StorageService: R2StorageService;
  s3StorageService: S3StorageService;
  storageFactoryService: StorageFactoryService;
  uploadFileHelper: UploadFileHelper;

  logReaderService: LogReaderService;
  runtimeMonitorService: RuntimeMonitorService;
  runtimeDbMetricsService: RuntimeDbMetricsService;
  runtimeProcessMetricsService: RuntimeProcessMetricsService;
  runtimeQueueMetricsService: RuntimeQueueMetricsService;
  meService: MeService;
  graphqlService: GraphqlService;
  dynamicResolver: DynamicResolver;

  flowService: FlowService;
  flowSchedulerService: FlowSchedulerService;
  flowExecutionQueueService: FlowExecutionQueueService;

  dynamicWebSocketGateway: DynamicWebSocketGateway;
  builtInSocketRegistry: BuiltInSocketRegistry;
  websocketGatewayFactory: WebsocketGatewayFactory;
  connectionQueueService: ConnectionQueueService;
  eventQueueService: EventQueueService;
  websocketEmitService: WebsocketEmitService;
  websocketContextFactory: WebsocketContextFactory;

  provisionService: ProvisionService;
  firstRunInitializer: FirstRunInitializer;
  metadataRepairService: MetadataRepairService;
  metadataProvisionService: MetadataProvisionService;
  metadataProvisionSqlService: MetadataProvisionSqlService;
  metadataProvisionMongoService: MetadataProvisionMongoService;
  dataProvisionService: DataProvisionService;
  dataMigrationService: DataMigrationService;
  metadataMigrationService: MetadataMigrationService;
  bootstrapScriptService: BootstrapScriptService;

  userDefinitionProcessor: UserDefinitionProcessor;
  menuDefinitionProcessor: MenuDefinitionProcessor;
  routeDefinitionProcessor: RouteDefinitionProcessor;
  routeHandlerDefinitionProcessor: RouteHandlerDefinitionProcessor;
  methodDefinitionProcessor: MethodDefinitionProcessor;
  preHookDefinitionProcessor: PreHookDefinitionProcessor;
  postHookDefinitionProcessor: PostHookDefinitionProcessor;
  fieldPermissionDefinitionProcessor: FieldPermissionDefinitionProcessor;
  hookDefinitionProcessor: HookDefinitionProcessor;
  settingDefinitionProcessor: SettingDefinitionProcessor;
  extensionDefinitionProcessor: ExtensionDefinitionProcessor;
  folderDefinitionProcessor: FolderDefinitionProcessor;
  bootstrapScriptDefinitionProcessor: BootstrapScriptDefinitionProcessor;
  routePermissionDefinitionProcessor: RoutePermissionDefinitionProcessor;
  websocketDefinitionProcessor: WebsocketDefinitionProcessor;
  websocketEventDefinitionProcessor: WebsocketEventDefinitionProcessor;
  flowDefinitionProcessor: FlowDefinitionProcessor;
  flowStepDefinitionProcessor: FlowStepDefinitionProcessor;
  flowExecutionDefinitionProcessor: FlowExecutionDefinitionProcessor;
  graphqlDefinitionProcessor: GraphQLDefinitionProcessor;
  genericTableProcessor: GenericTableProcessor;

  $req: any;
  $res: any;
  $body: any;
  $query: any;
  $params: any;
  $user: any;
  $ctx: any;
}

export function buildContainer(): AwilixContainer<Cradle> {
  const container = createContainer<Cradle>({
    injectionMode: InjectionMode.PROXY,
    strict: false,
  });

  container.register({
    envService: asClass(EnvService).singleton(),
    configService: asValue({
      get: (key: string, defaultValue?: any) =>
        (env as any)[key] ?? defaultValue,
      getOrThrow: (key: string) => {
        const v = (env as any)[key];
        if (v === undefined) throw new Error(`Config ${key} not found`);
        return v;
      },
    }),
    eventEmitter: asValue(
      new EventEmitter2({ wildcard: true, maxListeners: 50 }),
    ),
    redis: asFunction(() => new Redis(env.REDIS_URI))
      .singleton()
      .disposer((redis) => redis.disconnect()),

    flowQueue: asFunction(() => createRuntimeQueue(SYSTEM_QUEUES.FLOW_EXECUTION))
      .singleton()
      .disposer((queue) => closeRuntimeQueue(queue)),
    wsConnectionQueue: asFunction(() =>
      createRuntimeQueue(SYSTEM_QUEUES.WS_CONNECTION),
    )
      .singleton()
      .disposer((queue) => closeRuntimeQueue(queue)),
    wsEventQueue: asFunction(() => createRuntimeQueue(SYSTEM_QUEUES.WS_EVENT))
      .singleton()
      .disposer((queue) => closeRuntimeQueue(queue)),
    cleanupQueue: asFunction(() =>
      createRuntimeQueue(SYSTEM_QUEUES.SESSION_CLEANUP),
    )
      .singleton()
      .disposer((queue) => closeRuntimeQueue(queue)),

    commonService: asClass(CommonService).singleton(),
    databaseConfigService: asClass(DatabaseConfigService).singleton(),
    lazyRef: asFunction((cradle) => cradle).singleton(),
    instanceService: asClass(InstanceService).singleton(),
    dynamicContextFactory: asClass(DynamicContextFactory).singleton(),
    runtimeMetricsCollectorService: asClass(
      RuntimeMetricsCollectorService,
    ).singleton(),
    clusterTelemetryService: asClass(ClusterTelemetryService).singleton(),
    bcryptService: asClass(BcryptService).singleton(),
    authService: asClass(AuthService).singleton(),
    oauthService: asClass(OAuthService).singleton(),
    sessionCleanupService: asClass(SessionCleanupService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    userRevocationService: asClass(UserRevocationService).singleton(),
    loggingService: asClass(LoggingService).singleton(),
    policyService: asClass(PolicyService).singleton(),
    schemaMigrationValidatorService: asClass(
      SchemaMigrationValidatorService,
    ).singleton(),
    systemSafetyAuditorService: asClass(SystemSafetyAuditorService).singleton(),

    mongoService: asClass(MongoService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    mongoSchemaMigrationService: asClass(
      MongoSchemaMigrationService,
    ).singleton(),
    mongoSchemaMigrationLockService: asClass(
      MongoSchemaMigrationLockService,
    ).singleton(),
    mongoSagaLockService: asClass(MongoSagaLockService).singleton(),
    mongoSagaCoordinator: asFunction(
      (cradle) =>
        new MongoSagaCoordinator({
          mongoService: cradle.mongoService,
          lockService: cradle.mongoSagaLockService,
          logService: cradle.mongoOperationLogService,
          instanceService: cradle.instanceService,
          cacheService: cradle.cacheService,
        }),
    )
      .singleton()
      .disposer((service) => service.onDestroy()),
    mongoOperationLogService: asClass(MongoOperationLogService).singleton(),
    mongoMigrationJournalService: asClass(
      MongoMigrationJournalService,
    ).singleton(),
    mongoSchemaDiffService: asClass(MongoSchemaDiffService).singleton(),
    mongoRelationManagerService: asClass(
      MongoRelationManagerService,
    ).singleton(),

    knexService: asClass(KnexService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    knexHookManagerService: asClass(KnexHookManagerService).singleton(),
    replicationManager: asClass(ReplicationManager)
      .singleton()
      .disposer((service) => service.onDestroy()),
    sqlSchemaMigrationService: asClass(SqlSchemaMigrationService).singleton(),
    sqlSchemaDiffService: asClass(SqlSchemaDiffService).singleton(),
    migrationJournalService: asClass(MigrationJournalService).singleton(),
    databaseSchemaService: asClass(DatabaseSchemaService).singleton(),
    schemaMigrationLockService: asClass(SchemaMigrationLockService).singleton(),
    sqlPoolClusterCoordinatorService: asClass(SqlPoolClusterCoordinatorService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    sqlFunctionService: asClass(SqlFunctionService).singleton(),

    queryBuilderService: asClass(QueryBuilderService).singleton(),
    queryEngine: asClass(QueryEngine).singleton(),
    sqlQueryEngine: asClass(SqlQueryEngine).singleton(),
    mongoQueryEngine: asClass(MongoQueryEngine).singleton(),

    isolatedExecutorService: asClass(IsolatedExecutorService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    executorEngineService: asClass(ExecutorEngineService).singleton(),

    cacheService: asClass(CacheService).singleton(),
    redisPubSubService: asClass(RedisPubSubService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    metadataCacheService: asClass(MetadataCacheService).singleton(),
    routeCacheService: asClass(RouteCacheService).singleton(),
    packageCacheService: asClass(PackageCacheService).singleton(),
    storageConfigCacheService: asClass(StorageConfigCacheService).singleton(),
    websocketCacheService: asClass(WebsocketCacheService).singleton(),
    oauthConfigCacheService: asClass(OAuthConfigCacheService).singleton(),
    rateLimitService: asClass(RateLimitService).singleton(),
    folderTreeCacheService: asClass(FolderTreeCacheService).singleton(),
    flowCacheService: asClass(FlowCacheService).singleton(),
    packageCdnLoaderService: asClass(PackageCdnLoaderService).singleton(),
    guardCacheService: asClass(GuardCacheService).singleton(),
    guardEvaluatorService: asClass(GuardEvaluatorService).singleton(),
    settingCacheService: asClass(SettingCacheService).singleton(),
    fieldPermissionCacheService: asClass(
      FieldPermissionCacheService,
    ).singleton(),
    columnRuleCacheService: asClass(ColumnRuleCacheService).singleton(),
    gqlDefinitionCacheService: asClass(GqlDefinitionCacheService).singleton(),
    repoRegistryService: asClass(RepoRegistryService).singleton(),
    dynamicRepositoryFactory: asClass(DynamicRepositoryFactory).singleton(),
    cacheOrchestratorService: asClass(CacheOrchestratorService)
      .singleton()
      .disposer((service) => service.onDestroy()),

    tableHandlerService: asClass(TableHandlerService)
      .singleton()
      .inject((container: any) => ({
        sqlTableHandlerService: container.cradle.sqlTableHandlerService,
        mongoTableHandlerService: container.cradle.mongoTableHandlerService,
        databaseConfigService: container.cradle.databaseConfigService,
      })),
    sqlTableHandlerService: asClass(SqlTableHandlerService).singleton(),
    mongoTableHandlerService: asClass(MongoTableHandlerService).singleton(),
    tableValidationService: asClass(
      DynamicApiTableValidationService,
    ).singleton(),
    tableManagementValidationService: asClass(
      TableManagementValidationService,
    ).singleton(),
    mongoMetadataSnapshotService: asClass(
      MongoMetadataSnapshotService,
    ).singleton(),
    sqlTableMetadataBuilderService: asClass(
      SqlTableMetadataBuilderService,
    ).singleton(),
    sqlTableMetadataWriterService: asClass(
      SqlTableMetadataWriterService,
    ).singleton(),

    dynamicService: asClass(DynamicService).singleton(),

    fileManagementService: asClass(FileManagementService).singleton(),
    fileAssetsService: asClass(FileAssetsService).singleton(),
    localStorageService: asClass(LocalStorageService).singleton(),
    gcsStorageService: asClass(GCSStorageService).singleton(),
    r2StorageService: asClass(R2StorageService).singleton(),
    s3StorageService: asClass(S3StorageService).singleton(),
    storageFactoryService: asClass(StorageFactoryService).singleton(),
    uploadFileHelper: asClass(UploadFileHelper).singleton(),

    logReaderService: asClass(LogReaderService).singleton(),
    runtimeDbMetricsService: asClass(RuntimeDbMetricsService).singleton(),
    runtimeProcessMetricsService: asClass(RuntimeProcessMetricsService)
      .singleton()
      .disposer((service) => service.disable()),
    runtimeQueueMetricsService: asClass(RuntimeQueueMetricsService).singleton(),
    runtimeMonitorService: asClass(RuntimeMonitorService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    meService: asClass(MeService).singleton(),
    graphqlService: asClass(GraphqlService).singleton(),
    dynamicResolver: asClass(DynamicResolver).singleton(),

    flowService: asClass(FlowService).singleton(),
    flowSchedulerService: asClass(FlowSchedulerService).singleton(),
    flowExecutionQueueService: asClass(FlowExecutionQueueService)
      .singleton()
      .disposer((service) => service.onDestroy()),

    builtInSocketRegistry: asClass(BuiltInSocketRegistry).singleton(),
    dynamicWebSocketGateway: asClass(DynamicWebSocketGateway)
      .singleton()
      .disposer((service) => service.onDestroy()),
    websocketGatewayFactory: asClass(WebsocketGatewayFactory).singleton(),
    connectionQueueService: asClass(ConnectionQueueService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    eventQueueService: asClass(EventQueueService)
      .singleton()
      .disposer((service) => service.onDestroy()),
    websocketEmitService: asClass(WebsocketEmitService).singleton(),
    websocketContextFactory: asClass(WebsocketContextFactory).singleton(),

    provisionService: asClass(ProvisionService).singleton(),
    firstRunInitializer: asClass(FirstRunInitializer).singleton(),
    metadataRepairService: asClass(MetadataRepairService).singleton(),
    metadataProvisionService: asClass(MetadataProvisionService).singleton(),
    metadataProvisionSqlService: asClass(
      MetadataProvisionSqlService,
    ).singleton(),
    metadataProvisionMongoService: asClass(
      MetadataProvisionMongoService,
    ).singleton(),
    dataProvisionService: asClass(DataProvisionService).singleton(),
    dataMigrationService: asClass(DataMigrationService).singleton(),
    metadataMigrationService: asClass(MetadataMigrationService).singleton(),
    bootstrapScriptService: asClass(BootstrapScriptService).singleton(),

    userDefinitionProcessor: asClass(UserDefinitionProcessor).singleton(),
    menuDefinitionProcessor: asClass(MenuDefinitionProcessor).singleton(),
    routeDefinitionProcessor: asClass(RouteDefinitionProcessor).singleton(),
    routeHandlerDefinitionProcessor: asClass(
      RouteHandlerDefinitionProcessor,
    ).singleton(),
    methodDefinitionProcessor: asClass(MethodDefinitionProcessor).singleton(),
    preHookDefinitionProcessor: asClass(PreHookDefinitionProcessor).singleton(),
    postHookDefinitionProcessor: asClass(
      PostHookDefinitionProcessor,
    ).singleton(),
    fieldPermissionDefinitionProcessor: asClass(
      FieldPermissionDefinitionProcessor,
    ).singleton(),
    hookDefinitionProcessor: asClass(HookDefinitionProcessor).singleton(),
    settingDefinitionProcessor: asClass(SettingDefinitionProcessor).singleton(),
    extensionDefinitionProcessor: asClass(
      ExtensionDefinitionProcessor,
    ).singleton(),
    folderDefinitionProcessor: asClass(FolderDefinitionProcessor).singleton(),
    bootstrapScriptDefinitionProcessor: asClass(
      BootstrapScriptDefinitionProcessor,
    ).singleton(),
    routePermissionDefinitionProcessor: asClass(
      RoutePermissionDefinitionProcessor,
    ).singleton(),
    websocketDefinitionProcessor: asClass(
      WebsocketDefinitionProcessor,
    ).singleton(),
    websocketEventDefinitionProcessor: asClass(
      WebsocketEventDefinitionProcessor,
    ).singleton(),
    flowDefinitionProcessor: asClass(FlowDefinitionProcessor).singleton(),
    flowStepDefinitionProcessor: asClass(
      FlowStepDefinitionProcessor,
    ).singleton(),
    flowExecutionDefinitionProcessor: asClass(
      FlowExecutionDefinitionProcessor,
    ).singleton(),
    graphqlDefinitionProcessor: asClass(GraphQLDefinitionProcessor).singleton(),
    genericTableProcessor: asClass(GenericTableProcessor).singleton(),
  });

  return container;
}

export function buildRequestScope(
  root: AwilixContainer<Cradle>,
  req: any,
  res: any,
): AwilixContainer<Cradle> {
  const scope = root.createScope<Cradle>();
  scope.register({
    $req: asValue(req),
    $res: asValue(res),
    $body: asValue(req.body ?? {}),
    $query: asValue(req.query ?? {}),
    $params: asValue(req.params ?? {}),
    $user: asValue(null),
    $ctx: asValue(null),
  });
  return scope;
}
