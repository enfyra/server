import { asClass } from 'awilix';
import { DynamicService } from '../../modules/dynamic-api';
import {
  FileManagementService,
  FileAssetsService,
  GCSStorageService,
  LocalStorageService,
  R2StorageService,
  S3StorageService,
  StorageFactoryService,
} from '../../modules/file-management';
import { UploadFileHelper } from '../../shared/helpers';
import {
  LogReaderService,
  RuntimeMonitorService,
  RuntimeDbMetricsService,
  RuntimeProcessMetricsService,
  RuntimeQueueMetricsService,
  RedisAdminService,
} from '../../modules/admin';
import { MeService } from '../../modules/me';
import { GraphqlService, DynamicResolver } from '../../modules/graphql';
import {
  FlowExecutionQueueService,
  FlowQueueMaintenanceService,
  FlowRuntimeService,
  FlowSchedulerService,
  FlowTriggerDispatcherService,
  FlowService,
} from '../../modules/flow';
import {
  DynamicWebSocketGateway,
  BuiltInSocketRegistry,
  WebsocketEmitService,
  WebsocketContextFactory,
  WebsocketRuntimeService,
} from '../../modules/websocket';

export const dynamicRegisters = {
  dynamicService: asClass(DynamicService).singleton(),
} as const;

export const storageRegisters = {
  fileManagementService: asClass(FileManagementService).singleton(),
  fileAssetsService: asClass(FileAssetsService).singleton(),
  localStorageService: asClass(LocalStorageService).singleton(),
  gcsStorageService: asClass(GCSStorageService).singleton(),
  r2StorageService: asClass(R2StorageService).singleton(),
  s3StorageService: asClass(S3StorageService).singleton(),
  storageFactoryService: asClass(StorageFactoryService).singleton(),
  uploadFileHelper: asClass(UploadFileHelper).singleton(),
} as const;

export const adminRegisters = {
  logReaderService: asClass(LogReaderService).singleton(),
  runtimeDbMetricsService: asClass(RuntimeDbMetricsService).singleton(),
  runtimeProcessMetricsService: asClass(RuntimeProcessMetricsService)
    .singleton()
    .disposer((service: RuntimeProcessMetricsService) => service.onDestroy()),
  runtimeQueueMetricsService: asClass(RuntimeQueueMetricsService).singleton(),
  redisAdminService: asClass(RedisAdminService).singleton(),
  runtimeMonitorService: asClass(RuntimeMonitorService)
    .singleton()
    .disposer((service: RuntimeMonitorService) => service.onDestroy()),
  meService: asClass(MeService).singleton(),
  graphqlService: asClass(GraphqlService).singleton(),
  dynamicResolver: asClass(DynamicResolver).singleton(),
} as const;

export const flowRegisters = {
  flowService: asClass(FlowService).singleton(),
  flowQueueMaintenanceService: asClass(FlowQueueMaintenanceService).singleton(),
  flowRuntimeService: asClass(FlowRuntimeService).singleton(),
  flowSchedulerService: asClass(FlowSchedulerService).singleton(),
  flowTriggerDispatcherService: asClass(FlowTriggerDispatcherService).singleton(),
  flowExecutionQueueService: asClass(FlowExecutionQueueService)
    .singleton()
    .disposer((service: FlowExecutionQueueService) => service.onDestroy()),
} as const;

export const websocketRegisters = {
  builtInSocketRegistry: asClass(BuiltInSocketRegistry).singleton(),
  dynamicWebSocketGateway: asClass(DynamicWebSocketGateway)
    .singleton()
    .disposer((service: DynamicWebSocketGateway) => service.onDestroy()),
  websocketRuntimeService: asClass(WebsocketRuntimeService).singleton(),
  websocketEmitService: asClass(WebsocketEmitService).singleton(),
  websocketContextFactory: asClass(WebsocketContextFactory).singleton(),
} as const;
