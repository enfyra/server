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

import { BcryptService } from './core/auth/services/bcrypt.service';
import { AuthService } from './core/auth/services/auth.service';
import { OAuthService } from './core/auth/services/oauth.service';
import { SessionCleanupService } from './core/auth/services/session-cleanup.service';
import { UserRevocationService } from './core/auth/services/user-revocation.service';

import { BootstrapScriptDefinitionProcessor } from './core/bootstrap/processors/bootstrap-script-definition.processor';
import { ExtensionDefinitionProcessor } from './core/bootstrap/processors/extension-definition.processor';
import { FlowDefinitionProcessor } from './core/bootstrap/processors/flow-definition.processor';
import { FlowExecutionDefinitionProcessor } from './core/bootstrap/processors/flow-execution-definition.processor';
import { FlowStepDefinitionProcessor } from './core/bootstrap/processors/flow-step-definition.processor';
import { FolderDefinitionProcessor } from './core/bootstrap/processors/folder-definition.processor';
import { GenericTableProcessor } from './core/bootstrap/processors/generic-table.processor';
import { GraphQLDefinitionProcessor } from './core/bootstrap/processors/graphql-definition.processor';
import { HookDefinitionProcessor } from './core/bootstrap/processors/hook-definition.processor';
import { MenuDefinitionProcessor } from './core/bootstrap/processors/menu-definition.processor';
import { MethodDefinitionProcessor } from './core/bootstrap/processors/method-definition.processor';
import { PostHookDefinitionProcessor } from './core/bootstrap/processors/post-hook-definition.processor';
import { PreHookDefinitionProcessor } from './core/bootstrap/processors/pre-hook-definition.processor';
import { RouteDefinitionProcessor } from './core/bootstrap/processors/route-definition.processor';
import { RouteHandlerDefinitionProcessor } from './core/bootstrap/processors/route-handler-definition.processor';
import { RoutePermissionDefinitionProcessor } from './core/bootstrap/processors/route-permission-definition.processor';
import { SettingDefinitionProcessor } from './core/bootstrap/processors/setting-definition.processor';
import { UserDefinitionProcessor } from './core/bootstrap/processors/user-definition.processor';
import { WebsocketDefinitionProcessor } from './core/bootstrap/processors/websocket-definition.processor';
import { WebsocketEventDefinitionProcessor } from './core/bootstrap/processors/websocket-event-definition.processor';

import { BootstrapScriptService } from './core/bootstrap/services/bootstrap-script.service';
import { DataMigrationService } from './core/bootstrap/services/data-migration.service';
import { DataProvisionService } from './core/bootstrap/services/data-provision.service';
import { FirstRunInitializer } from './core/bootstrap/services/first-run-initializer.service';
import { MetadataMigrationService } from './core/bootstrap/services/metadata-migration.service';
import { MetadataProvisionMongoService } from './core/bootstrap/services/metadata-provision-mongo.service';
import { MetadataProvisionSqlService } from './core/bootstrap/services/metadata-provision-sql.service';
import { MetadataProvisionService } from './core/bootstrap/services/metadata-provision.service';
import { ProvisionService } from './core/bootstrap/services/provision.service';

import { LoggingService } from './core/exceptions/services/logging.service';

import { PolicyService } from './core/policy/policy.service';
import { SchemaMigrationValidatorService } from './core/policy/services/schema-migration-validator.service';
import { SystemSafetyAuditorService } from './core/policy/services/system-safety-auditor.service';

import { CacheOrchestratorService } from './infrastructure/cache/services/cache-orchestrator.service';
import { CacheService } from './infrastructure/cache/services/cache.service';
import { FieldPermissionCacheService } from './infrastructure/cache/services/field-permission-cache.service';
import { FlowCacheService } from './infrastructure/cache/services/flow-cache.service';
import { FolderTreeCacheService } from './infrastructure/cache/services/folder-tree-cache.service';
import { GqlDefinitionCacheService } from './infrastructure/cache/services/gql-definition-cache.service';
import { GuardCacheService } from './infrastructure/cache/services/guard-cache.service';
import { GuardEvaluatorService } from './infrastructure/cache/services/guard-evaluator.service';
import { MetadataCacheService } from './infrastructure/cache/services/metadata-cache.service';
import { OAuthConfigCacheService } from './infrastructure/cache/services/oauth-config-cache.service';
import { PackageCacheService } from './infrastructure/cache/services/package-cache.service';
import { PackageCdnLoaderService } from './infrastructure/cache/services/package-cdn-loader.service';
import { RateLimitService } from './infrastructure/cache/services/rate-limit.service';
import { RedisPubSubService } from './infrastructure/cache/services/redis-pubsub.service';
import { RepoRegistryService } from './infrastructure/cache/services/repo-registry.service';
import { DynamicRepository } from './modules/dynamic-api/repositories/dynamic.repository';
import { RouteCacheService } from './infrastructure/cache/services/route-cache.service';
import { SettingCacheService } from './infrastructure/cache/services/setting-cache.service';
import { StorageConfigCacheService } from './infrastructure/cache/services/storage-config-cache.service';
import { WebsocketCacheService } from './infrastructure/cache/services/websocket-cache.service';

import { ExecutorEngineService } from './infrastructure/executor-engine/services/executor-engine.service';
import { IsolatedExecutorService } from './infrastructure/executor-engine/services/isolated-executor.service';

import { KnexService } from './infrastructure/knex/knex.service';
import { KnexHookManagerService } from './infrastructure/knex/services/knex-hook-manager.service';
import { MigrationJournalService } from './infrastructure/knex/services/migration-journal.service';
import { ReplicationManager } from './infrastructure/knex/services/replication-manager.service';
import { SchemaMigrationLockService } from './infrastructure/knex/services/schema-migration-lock.service';
import { SqlPoolClusterCoordinatorService } from './infrastructure/knex/services/sql-pool-cluster-coordinator.service';
import { SqlSchemaDiffService } from './infrastructure/knex/services/sql-schema-diff.service';
import { SqlSchemaMigrationService } from './infrastructure/knex/services/sql-schema-migration.service';
import { DatabaseSchemaService } from './infrastructure/knex/services/database-schema.service';

import { MongoMigrationJournalService } from './infrastructure/mongo/services/mongo-migration-journal.service';
import { MongoOperationLogService } from './infrastructure/mongo/services/mongo-operation-log.service';
import { MongoRelationManagerService } from './infrastructure/mongo/services/mongo-relation-manager.service';
import { MongoSagaCoordinator } from './infrastructure/mongo/services/mongo-saga-coordinator.service';
import { MongoSagaLockService } from './infrastructure/mongo/services/mongo-saga-lock.service';
import { MongoSchemaDiffService } from './infrastructure/mongo/services/mongo-schema-diff.service';
import { MongoSchemaMigrationLockService } from './infrastructure/mongo/services/mongo-schema-migration-lock.service';
import { MongoSchemaMigrationService } from './infrastructure/mongo/services/mongo-schema-migration.service';
import { MongoService } from './infrastructure/mongo/services/mongo.service';

import { QueryBuilderService } from './infrastructure/query-builder/query-builder.service';

import { MongoQueryEngine } from './infrastructure/query-engine/services/mongo-query-engine.service';
import { QueryEngine } from './infrastructure/query-engine/services/query-engine.service';
import { SqlQueryEngine } from './infrastructure/query-engine/services/sql-query-engine.service';

import { SqlFunctionService } from './infrastructure/sql/services/sql-function.service';

import { LogReaderService } from './modules/admin/services/log-reader.service';

import { DynamicService } from './modules/dynamic-api/services/dynamic.service';
import { DynamicApiTableValidationService } from './modules/dynamic-api/services/table-validation.service';

import { FileManagementService } from './modules/file-management/services/file-management.service';
import { FileAssetsService } from './modules/file-management/services/file-assets.service';
import { GCSStorageService } from './modules/file-management/storage/gcs-storage.service';
import { LocalStorageService } from './modules/file-management/storage/local-storage.service';
import { R2StorageService } from './modules/file-management/storage/r2-storage.service';
import { S3StorageService } from './modules/file-management/storage/s3-storage.service';
import { StorageFactoryService } from './modules/file-management/storage/storage-factory.service';

import { FlowExecutionQueueService } from './modules/flow/queues/flow-execution-queue.service';
import { FlowSchedulerService } from './modules/flow/services/flow-scheduler.service';
import { FlowService } from './modules/flow/services/flow.service';

import { GraphqlService } from './modules/graphql/services/graphql.service';
import { DynamicResolver } from './modules/graphql/resolvers/dynamic.resolver';

import { MeService } from './modules/me/services/me.service';

import { MongoMetadataSnapshotService } from './modules/table-management/services/mongo-metadata-snapshot.service';
import { MongoTableHandlerService } from './modules/table-management/services/mongo-table-handler.service';
import { SqlTableHandlerService } from './modules/table-management/services/sql-table-handler.service';
import { SqlTableMetadataBuilderService } from './modules/table-management/services/sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './modules/table-management/services/sql-table-metadata-writer.service';
import { TableHandlerService } from './modules/table-management/services/table-handler.service';
import { TableManagementValidationService } from './modules/table-management/services/table-validation.service';

import { WebsocketGatewayFactory } from './modules/websocket/gateway/websocket-gateway.factory';
import { DynamicWebSocketGateway } from './modules/websocket/gateway/dynamic-websocket.gateway';
import { ConnectionQueueService } from './modules/websocket/queues/connection-queue.service';
import { EventQueueService } from './modules/websocket/queues/event-queue.service';
import { BuiltInSocketRegistry } from './modules/websocket/services/built-in-socket.registry';
import { WebsocketEmitService } from './modules/websocket/services/websocket-emit.service';

import { CommonService } from './shared/common/services/common.service';
import { DatabaseConfigService } from './shared/services/database-config.service';
import { UploadFileHelper } from './shared/helpers/upload-file.helper';
import { EnvService } from './shared/services/env.service';
import { InstanceService } from './shared/services/instance.service';


export interface Cradle {
  envService: EnvService;
  eventEmitter: EventEmitter2;
  redis: Redis;

  commonService: CommonService;
  instanceService: InstanceService;
  configService: any;
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
  gqlDefinitionCacheService: GqlDefinitionCacheService;
  repoRegistryService: RepoRegistryService;
  dynamicRepository: (tableName: string, context: any, enforceFieldPermission?: boolean) => DynamicRepository;
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

  provisionService: ProvisionService;
  firstRunInitializer: FirstRunInitializer;
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
      get: (key: string, defaultValue?: any) => (env as any)[key] ?? defaultValue,
      getOrThrow: (key: string) => {
        const v = (env as any)[key];
        if (v === undefined) throw new Error(`Config ${key} not found`);
        return v;
      },
    }),
    eventEmitter: asValue(
      new EventEmitter2({ wildcard: true, maxListeners: 50 }),
    ),
    redis: asValue(new Redis(env.REDIS_URI)),

    flowQueue: asValue(new Queue(SYSTEM_QUEUES.FLOW_EXECUTION, { prefix: `${env.NODE_NAME}:`, connection: new Redis(env.REDIS_URI) })),
    wsConnectionQueue: asValue(new Queue(SYSTEM_QUEUES.WS_CONNECTION, { prefix: `${env.NODE_NAME}:`, connection: new Redis(env.REDIS_URI) })),
    wsEventQueue: asValue(new Queue(SYSTEM_QUEUES.WS_EVENT, { prefix: `${env.NODE_NAME}:`, connection: new Redis(env.REDIS_URI) })),
    cleanupQueue: asValue(new Queue(SYSTEM_QUEUES.SESSION_CLEANUP, { prefix: `${env.NODE_NAME}:`, connection: new Redis(env.REDIS_URI) })),

    commonService: asClass(CommonService).singleton(),
    databaseConfigService: asClass(DatabaseConfigService).singleton(),
    instanceService: asClass(InstanceService).singleton(),
    bcryptService: asClass(BcryptService).singleton(),
    authService: asClass(AuthService).singleton(),
    oauthService: asClass(OAuthService).singleton(),
    sessionCleanupService: asClass(SessionCleanupService).singleton(),
    userRevocationService: asClass(UserRevocationService).singleton(),
    loggingService: asClass(LoggingService).singleton(),
    policyService: asClass(PolicyService).singleton(),
    schemaMigrationValidatorService: asClass(SchemaMigrationValidatorService).singleton(),
    systemSafetyAuditorService: asClass(SystemSafetyAuditorService).singleton(),

    mongoService: asClass(MongoService).singleton().inject((container) => ({ _container: container })),
    mongoSchemaMigrationService: asClass(MongoSchemaMigrationService).singleton(),
    mongoSchemaMigrationLockService: asClass(MongoSchemaMigrationLockService).singleton(),
    mongoSagaLockService: asClass(MongoSagaLockService).singleton(),
    mongoSagaCoordinator: asFunction((cradle) => new MongoSagaCoordinator({
      mongoService: cradle.mongoService,
      lockService: cradle.mongoSagaLockService,
      logService: cradle.mongoOperationLogService,
      instanceService: cradle.instanceService,
      cacheService: cradle.cacheService,
    })).singleton(),
    mongoOperationLogService: asClass(MongoOperationLogService).singleton(),
    mongoMigrationJournalService: asClass(MongoMigrationJournalService).singleton(),
    mongoSchemaDiffService: asClass(MongoSchemaDiffService).singleton(),
    mongoRelationManagerService: asClass(MongoRelationManagerService).singleton(),

    knexService: asClass(KnexService).singleton().inject((container) => ({ _container: container })),
    knexHookManagerService: asClass(KnexHookManagerService).singleton(),
    replicationManager: asClass(ReplicationManager).singleton(),
    sqlSchemaMigrationService: asClass(SqlSchemaMigrationService).singleton(),
    sqlSchemaDiffService: asClass(SqlSchemaDiffService).singleton(),
    migrationJournalService: asClass(MigrationJournalService).singleton(),
    databaseSchemaService: asClass(DatabaseSchemaService).singleton().inject((container) => ({ _container: container })),
    schemaMigrationLockService: asClass(SchemaMigrationLockService).singleton(),
    sqlPoolClusterCoordinatorService: asClass(SqlPoolClusterCoordinatorService).singleton(),
    sqlFunctionService: asClass(SqlFunctionService).singleton(),

    queryBuilderService: asClass(QueryBuilderService).singleton().inject((container) => ({ _container: container })),
    queryEngine: asClass(QueryEngine).singleton(),
    sqlQueryEngine: asClass(SqlQueryEngine).singleton(),
    mongoQueryEngine: asClass(MongoQueryEngine).singleton(),

    isolatedExecutorService: asClass(IsolatedExecutorService).singleton(),
    executorEngineService: asClass(ExecutorEngineService).singleton(),

    cacheService: asClass(CacheService).singleton(),
    redisPubSubService: asClass(RedisPubSubService).singleton(),
    metadataCacheService: asClass(MetadataCacheService).singleton().inject((container) => ({ _container: container })),
    routeCacheService: asClass(RouteCacheService).singleton(),
    packageCacheService: asClass(PackageCacheService).singleton().inject((container) => ({ _container: container })),
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
    fieldPermissionCacheService: asClass(FieldPermissionCacheService).singleton(),
    gqlDefinitionCacheService: asClass(GqlDefinitionCacheService).singleton(),
    repoRegistryService: asClass(RepoRegistryService).singleton(),
    dynamicRepository: asFunction((cradle: any) => {
      return (tableName: string, context: any, enforceFieldPermission?: boolean) => {
        return new DynamicRepository({
          tableName,
          context,
          enforceFieldPermission,
          tableHandlerService: cradle.tableHandlerService,
          queryBuilderService: cradle.queryBuilderService,
          queryEngine: cradle.queryEngine,
          metadataCacheService: cradle.metadataCacheService,
          policyService: cradle.policyService,
          tableValidationService: cradle.tableValidationService,
          settingCacheService: cradle.settingCacheService,
          fieldPermissionCacheService: cradle.fieldPermissionCacheService,
          userRevocationService: cradle.userRevocationService,
          eventEmitter: cradle.eventEmitter,
        });
      };
    }).singleton(),
    cacheOrchestratorService: asClass(CacheOrchestratorService).singleton(),

    tableHandlerService: asClass(TableHandlerService).singleton().inject((container: any) => ({
      sqlTableHandlerService: container.cradle.sqlTableHandlerService,
      mongoTableHandlerService: container.cradle.mongoTableHandlerService,
      databaseConfigService: container.cradle.databaseConfigService,
    })),
    sqlTableHandlerService: asClass(SqlTableHandlerService).singleton(),
    mongoTableHandlerService: asClass(MongoTableHandlerService).singleton(),
    tableValidationService: asClass(DynamicApiTableValidationService).singleton(),
    tableManagementValidationService: asClass(TableManagementValidationService).singleton(),
    mongoMetadataSnapshotService: asClass(MongoMetadataSnapshotService).singleton(),
    sqlTableMetadataBuilderService: asClass(SqlTableMetadataBuilderService).singleton(),
    sqlTableMetadataWriterService: asClass(SqlTableMetadataWriterService).singleton(),

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
    meService: asClass(MeService).singleton(),
    graphqlService: asClass(GraphqlService).singleton(),
    dynamicResolver: asClass(DynamicResolver).singleton(),

    flowService: asClass(FlowService).singleton(),
    flowSchedulerService: asClass(FlowSchedulerService).singleton(),
    flowExecutionQueueService: asClass(FlowExecutionQueueService).singleton(),

    builtInSocketRegistry: asClass(BuiltInSocketRegistry).singleton(),
    dynamicWebSocketGateway: asClass(DynamicWebSocketGateway).singleton(),
    websocketGatewayFactory: asClass(WebsocketGatewayFactory).singleton(),
    connectionQueueService: asClass(ConnectionQueueService).singleton().inject((container) => ({ _container: container, envService: (container.cradle as Cradle).envService })),
    eventQueueService: asClass(EventQueueService).singleton().inject((container) => ({ _container: container, envService: (container.cradle as Cradle).envService })),
    websocketEmitService: asClass(WebsocketEmitService).singleton(),

    provisionService: asClass(ProvisionService).singleton(),
    firstRunInitializer: asClass(FirstRunInitializer).singleton(),
    metadataProvisionService: asClass(MetadataProvisionService).singleton(),
    metadataProvisionSqlService: asClass(MetadataProvisionSqlService).singleton(),
    metadataProvisionMongoService: asClass(MetadataProvisionMongoService).singleton(),
    dataProvisionService: asClass(DataProvisionService).singleton(),
    dataMigrationService: asClass(DataMigrationService).singleton(),
    metadataMigrationService: asClass(MetadataMigrationService).singleton(),
    bootstrapScriptService: asClass(BootstrapScriptService).singleton(),

    userDefinitionProcessor: asClass(UserDefinitionProcessor).singleton(),
    menuDefinitionProcessor: asClass(MenuDefinitionProcessor).singleton(),
    routeDefinitionProcessor: asClass(RouteDefinitionProcessor).singleton(),
    routeHandlerDefinitionProcessor: asClass(RouteHandlerDefinitionProcessor).singleton(),
    methodDefinitionProcessor: asClass(MethodDefinitionProcessor).singleton(),
    preHookDefinitionProcessor: asClass(PreHookDefinitionProcessor).singleton(),
    postHookDefinitionProcessor: asClass(PostHookDefinitionProcessor).singleton(),
    hookDefinitionProcessor: asClass(HookDefinitionProcessor).singleton(),
    settingDefinitionProcessor: asClass(SettingDefinitionProcessor).singleton(),
    extensionDefinitionProcessor: asClass(ExtensionDefinitionProcessor).singleton(),
    folderDefinitionProcessor: asClass(FolderDefinitionProcessor).singleton(),
    bootstrapScriptDefinitionProcessor: asClass(BootstrapScriptDefinitionProcessor).singleton(),
    routePermissionDefinitionProcessor: asClass(RoutePermissionDefinitionProcessor).singleton(),
    websocketDefinitionProcessor: asClass(WebsocketDefinitionProcessor).singleton(),
    websocketEventDefinitionProcessor: asClass(WebsocketEventDefinitionProcessor).singleton(),
    flowDefinitionProcessor: asClass(FlowDefinitionProcessor).singleton(),
    flowStepDefinitionProcessor: asClass(FlowStepDefinitionProcessor).singleton(),
    flowExecutionDefinitionProcessor: asClass(FlowExecutionDefinitionProcessor).singleton(),
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
