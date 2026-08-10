import { asClass, asFunction, asValue } from 'awilix';
import { EventEmitter2 } from 'eventemitter2';
import Redis from 'ioredis';
import { Queue } from 'bullmq';

import { env, type Env } from '../../env';
import type { ConfigService } from '../../shared/interfaces/config-service.interface';
import type { Cradle } from '../cradle';
import { SYSTEM_QUEUES } from '../../shared/utils/constant';
import { EnvService } from '../../shared/services';
import { CommonService } from '../../shared/common';
import {
  DatabaseConfigService,
  InstanceService,
  DynamicContextFactory,
  RuntimeMetricsCollectorService,
  ClusterTelemetryService,
} from '../../shared/services';
import { createRuntimeQueue, closeRuntimeQueue } from '../queues';

const configService: ConfigService = {
  get: (key, defaultValue) => env[key as keyof Env] ?? defaultValue,
  getOrThrow: (key) => {
    const value = env[key as keyof Env];
    if (value === undefined) throw new Error(`Config ${key} not found`);
    return value;
  },
};

export const coreRegisters = {
  envService: asClass(EnvService).singleton(),
  configService: asValue(configService),
  eventEmitter: asValue(new EventEmitter2({ wildcard: true, maxListeners: 50 })),
  redis: asFunction(() => new Redis(env.REDIS_URI)).singleton(),

  flowQueue: asFunction(() => createRuntimeQueue(SYSTEM_QUEUES.FLOW_EXECUTION))
    .singleton()
    .disposer((queue: Queue) => closeRuntimeQueue(queue)),
  wsConnectionQueue: asFunction(() => createRuntimeQueue(SYSTEM_QUEUES.WS_CONNECTION))
    .singleton()
    .disposer((queue: Queue) => closeRuntimeQueue(queue)),
  wsEventQueue: asFunction(() => createRuntimeQueue(SYSTEM_QUEUES.WS_EVENT))
    .singleton()
    .disposer((queue: Queue) => closeRuntimeQueue(queue)),
  cleanupQueue: asFunction(() => createRuntimeQueue(SYSTEM_QUEUES.SESSION_CLEANUP))
    .singleton()
    .disposer((queue: Queue) => closeRuntimeQueue(queue)),
  mongoPhysicalMigrationQueue: asFunction(() =>
    createRuntimeQueue(SYSTEM_QUEUES.MONGO_PHYSICAL_MIGRATION),
  )
    .singleton()
    .disposer((queue: Queue) => closeRuntimeQueue(queue)),

  commonService: asClass(CommonService).singleton(),
  databaseConfigService: asClass(DatabaseConfigService).singleton(),
  lazyRef: asFunction((cradle: Cradle) => cradle).singleton(),
  instanceService: asClass(InstanceService).singleton(),
  dynamicContextFactory: asClass(DynamicContextFactory).singleton(),
  runtimeMetricsCollectorService: asClass(RuntimeMetricsCollectorService)
    .singleton()
    .disposer((service: RuntimeMetricsCollectorService) => service.onDestroy()),
  clusterTelemetryService: asClass(ClusterTelemetryService).singleton(),
} as const;
