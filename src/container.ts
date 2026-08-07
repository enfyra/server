/**
 * Container wiring — thin facade.
 *
 * Actual registrations live in `src/wiring/registers/*` grouped by domain.
 * `src/wiring/cradle.ts` owns the `Cradle` type.
 * `src/wiring/queues.ts` owns BullMQ queue helpers.
 *
 * This file preserves the public import path `import { buildContainer, Cradle } from './container'`
 * so no consumer needs to change.
 */
import { createContainer, asValue, InjectionMode, type AwilixContainer } from 'awilix';

import { coreRegisters } from './wiring/registers/core';
import { authRegisters } from './wiring/registers/auth';
import { policySchemaRegisters } from './wiring/registers/policy-schema';
import { mongoRegisters } from './wiring/registers/mongo';
import { sqlRegisters } from './wiring/registers/sql';
import { kernelExecutorRegisters } from './wiring/registers/kernel-executor';
import { cacheRegisters } from './wiring/registers/cache';
import { tableManagementRegisters } from './wiring/registers/table-management';
import {
  dynamicRegisters,
  storageRegisters,
  adminRegisters,
  flowRegisters,
  websocketRegisters,
} from './wiring/registers/modules';
import {
  bootstrapInfraRegisters,
  bootstrapProcessorRegisters,
} from './wiring/registers/bootstrap';

export type { Cradle } from './wiring/cradle';

export function buildContainer(): AwilixContainer<import('./wiring/cradle').Cradle> {
  const container = createContainer<import('./wiring/cradle').Cradle>({
    injectionMode: InjectionMode.PROXY,
    strict: false,
  });

  container.register({
    ...coreRegisters,
    ...authRegisters,
    ...policySchemaRegisters,
    ...mongoRegisters,
    ...sqlRegisters,
    ...kernelExecutorRegisters,
    ...cacheRegisters,
    ...tableManagementRegisters,
    ...dynamicRegisters,
    ...storageRegisters,
    ...adminRegisters,
    ...flowRegisters,
    ...websocketRegisters,
    ...bootstrapInfraRegisters,
    ...bootstrapProcessorRegisters,
  });

  return container;
}

export function buildRequestScope(
  root: AwilixContainer<import('./wiring/cradle').Cradle>,
  req: any,
  res: any,
): AwilixContainer<import('./wiring/cradle').Cradle> {
  const scope = root.createScope<import('./wiring/cradle').Cradle>();
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
